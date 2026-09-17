"""Gmail source adapter. Transport is read-only; source commits belong to SourceSync."""
import base64
import json
import threading
import time
from datetime import datetime, timezone
from email.utils import getaddresses, parsedate_to_datetime
from urllib.parse import urlencode, quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from .common import digest, now
from .gmail_oauth import token_request
from .importers import PlainHTML
from .source_sdk import (SourceAdapter, AdapterError, adapter_spec, stream_spec,
                         normalized_item, read_state, source_page, source_operation)


def decode(data):
    return base64.urlsafe_b64decode(data + '=' * (-len(data) % 4))


class GmailAdapter(SourceAdapter):
    def __init__(self, transport=None):
        self.transport=transport
        self.tokens={}
        self.lock=threading.Lock()

    def spec(self):
        return adapter_spec('google.gmail','1.0', capabilities={
            'history':True,'incremental':True,'reconciliation':True,'deletions':True,'attachments':True},
            secret_refs=['oauth'])

    def discover(self, context):
        return [stream_spec('messages',modes=['backfill','incremental','reconcile'],version_order='integer')]

    def _access(self, context, invalidate=False):
        credential=json.loads(context['secrets'](context['secret_ref']))
        key=digest(credential)
        with self.lock:
            cached=self.tokens.get(context['connection_id'])
            if not invalidate and cached and cached[0]==key and cached[2]>time.monotonic()+60:
                return cached[1]
            try:
                token=token_request({**{k:credential[k] for k in ('client_id','client_secret','refresh_token')},
                                     'grant_type':'refresh_token'})
            except (KeyError,ValueError):
                raise AdapterError('auth','Gmail token refresh failed; authorize this connection again') from None
            self.tokens[context['connection_id']]=(key,token['access_token'],time.monotonic()+int(token.get('expires_in',3600)))
            return token['access_token']

    def request(self, context, path, params=None):
        if context.get('cancelled',lambda:False)():
            raise AdapterError('temporary','Source worker is stopping')
        if self.transport: return self.transport(context,path,params or {})
        url='https://gmail.googleapis.com/gmail/v1/users/me/'+path
        if params: url+='?'+urlencode(params)
        for attempt in range(2):
            try:
                token=self._access(context,invalidate=attempt>0)
                with urlopen(Request(url,headers={'Authorization':'Bearer '+token}),timeout=20) as response:
                    raw=response.read(64*1024*1024+1)
                    if len(raw)>64*1024*1024: raise AdapterError('permanent','Gmail response exceeds the supported size')
                    return json.loads(raw)
            except HTTPError as error:
                if error.code==401 and attempt==0: continue
                kind='auth' if error.code in (401,403) else 'rate_limit' if error.code==429 else 'temporary' if error.code>=500 else 'cursor' if path=='history' and error.code==404 else 'permanent'
                retry=error.headers.get('Retry-After','')
                failure=AdapterError(kind,f'Gmail {path.split("/")[0]} request failed (HTTP {error.code})',
                                     retry_after=min(float(retry),3600) if retry.isdigit() else None)
                failure.status=error.code
                raise failure from None
            except (URLError,TimeoutError):
                raise AdapterError('temporary','Gmail request timed out or could not connect') from None

    def check(self, context):
        profile=self.request(context,'profile')
        expected=context['scope'].get('account_id')
        if expected and profile['emailAddress'].casefold()!=expected.casefold():
            raise AdapterError('auth','Gmail account does not match the configured connection')
        return {'account_id':profile['emailAddress'],'history_id':profile['historyId'],
                'messages_total':profile['messagesTotal'],'read_only':True}

    def normalize(self, payload):
        message=payload['message']; source=payload['source']
        root=message.get('payload',{}); headers=root.get('headers',[])
        values={}
        for h in headers: values.setdefault(h['name'].lower(),[]).append(h['value'])
        participants=[]
        for name in ('from','to','cc','bcc'):
            for label,address in getaddresses(values.get(name,[])):
                if '@' in address:
                    participants.append({'namespace':'email','address':address,'label':label or address,'relation':name})
        attachments=[]
        def body(part):
            mime=part.get('mimeType',''); content=part.get('body',{}); filename=part.get('filename','')
            if filename or mime not in ('text/plain','text/html') and content.get('attachmentId'):
                attachments.append({'message_id':message['id'],'part_id':part.get('partId','0'),
                    'attachment_id':content.get('attachmentId'),'filename':filename or 'attachment',
                    'mime':mime or 'application/octet-stream','size':content.get('size',0)})
                return ''
            children=part.get('parts',[])
            if children:
                if mime=='multipart/alternative':
                    selected=next((p for p in children if p.get('mimeType')=='text/plain'),None)
                    if selected is not None: return body(selected)
                return '\n'.join(filter(None,(body(p) for p in children)))
            if mime in ('text/plain','text/html'):
                if content.get('attachmentId') and not content.get('data'):
                    raise AdapterError('temporary','Message text body must be fetched before normalization')
                raw=decode(content.get('data',''))
                charset='utf-8'
                from email.message import Message
                for header in part.get('headers',[]):
                    if header['name'].lower()=='content-type':
                        m=Message();m['Content-Type']=header['value'];charset=m.get_content_charset() or charset
                try:text=raw.decode(charset,errors='replace')
                except LookupError:text=raw.decode('utf-8',errors='replace')
                if mime=='text/html':
                    parser=PlainHTML();parser.feed(text);text=''.join(parser.parts)
                return text
            return ''
        text=body(root)
        prefix='\n'.join(f'{name}: {", ".join(values[name])}' for name in ('from','to','cc','subject','date','message-id','in-reply-to','references') if name in values)
        text=prefix+'\n\n'+(text or '[No text body; see attachment evidence]')
        occurred=None
        try: occurred=datetime.fromtimestamp(int(message['internalDate'])/1000,timezone.utc).isoformat()
        except (KeyError,ValueError,OverflowError):
            try:
                date=parsedate_to_datetime(values.get('date',[''])[0])
                if date.tzinfo is not None: occurred=date.isoformat()
            except (ValueError,TypeError,IndexError):pass
        stable={'thread_id':message.get('threadId'),'headers':headers,'internal_date':message.get('internalDate'),
                'attachments':attachments}
        # Label/history changes do not change immutable content identity or embeddings.
        revision=digest([text,stable,occurred])[:32]
        records=[]; observed=now()
        for index,start in enumerate(range(0,len(text),90000)):
            records.append({'schema_version':'1.0','source':source,'source_id':message['id'],
                'revision':revision+':'+str(index),'kind':'email','occurred_at':occurred,'observed_at':observed,
                'text':text[start:start+90000],'participants':participants,
                'provenance':{'connector_id':'google.gmail','connector_version':'1.0',
                    'source_locator':'https://mail.google.com/mail/u/0/#all/'+message['id'],
                    'origin':'source','parent_record_ids':[]},
                'extensions':{'google.gmail':{'version':'1.0','data':{**stable,'part':index,'offset':start}}}})
        return normalized_item(message['id'],records=records,head_version=message.get('historyId'),attachments=attachments)

    def _message(self, context, mid, version=None, *, added=False):
        try: message=self.request(context,'messages/'+quote(mid,safe=''),{'format':'full'})
        except AdapterError as error:
            if getattr(error,'status',None)==404:
                return source_operation('remove',mid,source_version=version,reason='unavailable_at_fetch')
            raise
        if set(message.get('labelIds',[])) & {'SPAM','TRASH'}:
            return source_operation('remove',mid,source_version=message.get('historyId') or version,reason='excluded_label')
        def hydrate(part):
            content=part.get('body',{})
            if not part.get('filename') and part.get('mimeType') in ('text/plain','text/html') and content.get('attachmentId'):
                content['data']=self.request(context,'messages/'+quote(mid,safe='')+'/attachments/'+quote(content['attachmentId'],safe=''))['data']
            for child in part.get('parts',[]):hydrate(child)
        hydrate(message.get('payload',{}))
        item=self.normalize({'message':message,'source':context['source']})
        coordinates={'conversation_key':message['threadId']} if message.get('threadId') else {}
        if added:
            coordinates['arrival']='fresh'
        return source_operation('upsert',mid,records=item['records'],source_version=item['head_version'],
                                coordinates=coordinates,
                                metadata={'labels':message.get('labelIds',[]),
                                          'thread_id':message.get('threadId')},attachments=item['attachments'])

    def read_page(self, context, state):
        self.accept_state(state,supported_state_version=1)
        cursor=state['cursor'] or {}; mode=state['mode']; limit=20
        if mode in ('backfill','reconcile'):
            if cursor.get('done'):
                return source_page(page_id='done',operations=[],next_state=read_state(cursor=cursor,mode=mode))
            if 'pending' in cursor:
                pending=cursor['pending'];following=cursor.get('following')
            else:
                params={'maxResults':limit,'includeSpamTrash':'false'}
                if context['scope'].get('after'):params['q']='after:'+context['scope']['after']
                if cursor.get('page'):params['pageToken']=cursor['page']
                response=self.request(context,'messages',params)
                pending=[m['id'] for m in response.get('messages',[])]
                following=response.get('nextPageToken')
            operations=[];bytes_used=0
            for mid in pending:
                operation=self._message(context,mid)
                size=len(json.dumps(operation,ensure_ascii=False).encode())
                if operations and bytes_used+size>12*1024*1024:break
                operations.append(operation);bytes_used+=size
            remaining=pending[len(operations):]
            next_cursor=({'pending':remaining,'following':following,'count':cursor.get('count',0)+len(operations)}
                         if remaining else {'page':following,'done':not following,
                                            'count':cursor.get('count',0)+len(operations)})
            identity=[cursor,next_cursor,[(o['source_id'],o['source_version'],o['action'],
                     digest(o.get('records',[]))) for o in operations]]
            return source_page(page_id='backfill-'+digest(identity)[:24],operations=operations,
                next_state=read_state(cursor=next_cursor,mode=mode))
        # An offset into a repeatable history page keeps the durable cursor bounded
        # even when one upstream history entry contains thousands of changed IDs.
        anchor=cursor.get('history') or context['scope']['initial_history']
        params={'startHistoryId':anchor,'maxResults':20}
        if cursor.get('page'):params['pageToken']=cursor['page']
        response=self.request(context,'history',params)
        latest=response.get('historyId',anchor); page_token=response.get('nextPageToken')
        # Freeze the page's history boundary while draining an oversized entry.
        # New changes to already-processed IDs are consumed on the next delta pass.
        boundary=cursor.get('boundary') or latest
        changes={}
        for history in response.get('history',[]):
            if int(history['id'])>int(boundary):continue
            for field,action in (('messagesAdded','upsert'),('labelsAdded','upsert'),('labelsRemoved','upsert'),('messagesDeleted','remove')):
                for entry in history.get(field,[]):
                    mid=entry['message']['id']
                    prior=changes.get(mid)
                    changes[mid]={'id':mid,'action':action,'version':history['id'],
                                  'added':action != 'remove' and (field == 'messagesAdded' or bool(prior and prior.get('added')))}
        changes=list(changes.values()); offset=cursor.get('offset',0)
        operations=[];bytes_used=0
        for change in changes[offset:offset+limit]:
            operation=(source_operation('remove',change['id'],source_version=change['version'])
                       if change['action']=='remove' else self._message(
                           context,change['id'],change['version'],added=change['added']))
            size=len(json.dumps(operation,ensure_ascii=False).encode())
            if operations and bytes_used+size>12*1024*1024:break
            operations.append(operation);bytes_used+=size
        if offset+len(operations)<len(changes):
            next_cursor={'history':anchor,'page':cursor.get('page'),'offset':offset+len(operations),'boundary':boundary}
        elif page_token:
            next_cursor={'history':anchor,'page':page_token,'boundary':boundary}
        else:next_cursor={'history':boundary}
        identity=[cursor,next_cursor,[(o['source_id'],o['source_version'],o['action']) for o in operations]]
        return source_page(page_id='history-'+digest(identity)[:24],operations=operations,
                           next_state=read_state(cursor=next_cursor,mode=mode))

    def attachment(self, context, descriptor):
        mid=quote(descriptor['message_id'],safe='')
        if descriptor.get('attachment_id'):
            response=self.request(context,'messages/'+mid+'/attachments/'+quote(descriptor['attachment_id'],safe=''))
            return decode(response['data'])
        message=self.request(context,'messages/'+mid,{'format':'full'})
        def find(part):
            if part.get('partId','0')==descriptor['part_id']:
                return part.get('body',{}).get('data')
            for child in part.get('parts',[]):
                data=find(child)
                if data is not None:return data
            return None
        data=find(message.get('payload',{}))
        if data is None:raise AdapterError('temporary','Inline attachment is unavailable')
        return decode(data)

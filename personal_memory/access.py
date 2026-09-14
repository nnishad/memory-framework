"""Recipient boundary for a single owner's profile; host metadata is trusted input."""

def session_allowed(policy, context):
    if not isinstance(policy,dict):raise ValueError('session_access must be an object')
    owners=policy.get('owners',{})
    if not isinstance(owners,dict) or any(not isinstance(v,list) or any(not isinstance(x,str) or not x for x in v) for v in owners.values()):
        raise ValueError('owners must map platform names to lists of stable user IDs')
    recipients = policy.get('cron_recipients', [])
    if not isinstance(recipients, list): raise ValueError('cron_recipients must be a list')
    for entry in recipients:
        if (not isinstance(entry, dict) or set(entry) != {'platform','chat_id','thread_id','chat_type'}
                or entry['chat_type'] != 'private' or not isinstance(entry['platform'], str) or not entry['platform']
                or not isinstance(entry['chat_id'], str) or not entry['chat_id'] or not isinstance(entry['thread_id'], str)):
            raise ValueError('Cron recipients require explicit string platform/chat_id/thread_id and private chat_type')
    platform=context.get('platform','')
    chat_type=str(context.get('chat_type') or '').lower()
    if chat_type in {'group','supergroup','channel','guild','public','room'}:return False
    # CLI and cron execute under the profile owner's OS account. Do not infer
    # trust for arbitrary gateway, API or GUI sessions with missing identity.
    if platform in {'cli','cron'} and not chat_type:return True
    if chat_type not in {'private','dm','direct'}:return False
    return str(context.get('user_id') or '') in owners.get(platform,[])

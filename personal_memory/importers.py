"""Offline exports to canonical records. No account scraping or implicit uploads."""
import csv
import json
import mailbox
import re
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses, parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from zoneinfo import ZoneInfo

from .common import digest, timestamp


class PlainHTML(HTMLParser):
    def __init__(self):
        super().__init__(); self.parts=[]; self.hidden=0
    def handle_starttag(self,tag,attrs):
        if tag in {"script","style"}: self.hidden+=1
        if tag in {"br","p","div","li"}: self.parts.append("\n")
    def handle_endtag(self,tag):
        if tag in {"script","style"}: self.hidden=max(0,self.hidden-1)
    def handle_data(self,data):
        if not self.hidden: self.parts.append(data)


def emails(path, source="email"):
    path=Path(path)
    if path.suffix.lower()==".eml":
        messages=[BytesParser(policy=policy.default).parsebytes(path.read_bytes())]
    else:
        box=mailbox.mbox(path,create=False,factory=lambda f:BytesParser(policy=policy.default).parse(f))
        messages=box
    try:
        for message in messages:
            when=parsedate_to_datetime(str(message.get("Date","")))
            if when.tzinfo is None:
                raise ValueError("Email Date has no timezone; normalize explicitly before importing")
            body=message.get_body(preferencelist=("plain","html"))
            text=body.get_content() if body else "[No text body; attachments not imported]"
            if body and body.get_content_type()=="text/html":
                parser=PlainHTML(); parser.feed(text); text="".join(parser.parts)
            participants=[]
            for header in ("From","To","Cc"):
                for label,address in getaddresses([str(x) for x in message.get_all(header,[])]):
                    if address:
                        participants.append({"namespace":"email","address":address,"label":label or address,"relation":header.lower()})
            headers={k:str(message.get(k,"")) for k in ("From","To","Cc","Subject","Date","Message-ID","In-Reply-To","References")}
            content="\n".join(f"{k}: {v}" for k,v in headers.items() if v)+"\n\n"+text
            sid=headers["Message-ID"].strip() or "sha256:"+digest([headers,text])
            yield {"source":source,"source_id":sid,"occurred_at":when.isoformat(),"text":content,"kind":"email",
                   "metadata":{"headers":headers,"participants":participants,
                               "attachments":[str(p.get_filename()) for p in message.iter_attachments()],
                               "attachment_contents_imported":False,"quoted_content_preserved":True}}
    finally:
        if path.suffix.lower()!=".eml": box.close()


HEADER=re.compile(r"^\[?(\d{1,4}[./-]\d{1,2}[./-]\d{1,4}),?\s+(\d{1,2}:\d{2}(?::\d{2})?(?:\s*[APap][Mm])?)(?:\]\s*|\s+-\s+)(.*)$")


def whatsapp(path, thread_id, date_order, timezone_name, source="whatsapp", fold=0):
    if date_order not in {"DMY","MDY","YMD"}: raise ValueError("Choose DMY, MDY or YMD")
    if fold not in {0,1}: raise ValueError("fold must be 0 or 1")
    zone=ZoneInfo(timezone_name); counts=Counter(); current=None; preamble=[]

    def convert(row):
        date,clock,lines,raw_lines=row
        parts=[int(x) for x in re.split(r"[./-]",date)]
        values=dict(zip(date_order,parts)); year=values["Y"]
        if year<100: year+=2000 if year<70 else 1900
        clock=clock.upper().replace("\u202f"," ").strip()
        clock=re.sub(r"\s*([AP]M)$",r" \1",clock)
        fmt="%I:%M:%S %p" if "M" in clock and clock.count(":")==2 else "%I:%M %p" if "M" in clock else "%H:%M:%S" if clock.count(":")==2 else "%H:%M"
        t=datetime.strptime(clock,fmt).time()
        naive=datetime(year,values["M"],values["D"],t.hour,t.minute,t.second)
        local=naive.replace(tzinfo=zone,fold=fold)
        if local.astimezone(timezone.utc).astimezone(zone).replace(tzinfo=None)!=naive:
            raise ValueError("WhatsApp timestamp falls in a nonexistent daylight-saving interval")
        payload="\n".join(lines); sender,sep,_=payload.partition(": ")
        participants=[]
        if sep:
            phone=re.sub(r"[\s().-]","",sender)
            namespace="phone" if re.fullmatch(r"\+[1-9][0-9]{6,14}",phone) else "whatsapp-label:"+thread_id
            participants=[{"namespace":namespace,"address":phone if namespace=="phone" else sender,"label":sender,"relation":"author"}]
        raw="\n".join(raw_lines)
        key=digest([thread_id,local.isoformat(),payload]); ordinal=counts[key]; counts[key]+=1
        return {"source":source,"source_id":thread_id+"/"+key+"/"+str(ordinal),"occurred_at":local.isoformat(),
                "kind":"whatsapp_message" if sep else "whatsapp_system","text":payload,
                "metadata":{"thread_id":thread_id,"participants":participants,"raw_export_entry":raw,
                            "date_order":date_order,"timezone":timezone_name,"fold":fold,
                            "ambiguous_local_time":naive.replace(tzinfo=zone,fold=0).utcoffset()!=naive.replace(tzinfo=zone,fold=1).utcoffset(),
                            "export_preamble":preamble,"media_contents_imported":False}}

    with Path(path).open(encoding="utf-8-sig") as stream:
        for line in stream:
            raw=line.rstrip("\r\n")
            normalized=raw.replace("\u200e","").replace("\u200f","").replace("\u202f"," ")
            match=HEADER.match(normalized)
            if match:
                if current: yield convert(current)
                current=(match[1],match[2],[match[3]],[raw])
            elif current:
                current[2].append(raw); current[3].append(raw)
            else: preamble.append(raw)
        if current: yield convert(current)
        else: raise ValueError("No supported WhatsApp timestamps found; input was not imported")


def health_csv(path, source="health"):
    with Path(path).open(encoding="utf-8-sig",newline="") as stream:
        reader=csv.DictReader(stream)
        if not {"timestamp","metric","value","unit"}.issubset(reader.fieldnames or []):
            raise ValueError("Health CSV requires timestamp, metric, value, unit; optional id and device")
        for row in reader:
            when=timestamp(row["timestamp"])
            try: value=Decimal(row["value"])
            except InvalidOperation: raise ValueError("Invalid health value") from None
            if not value.is_finite() or not row["metric"].strip() or not row["unit"].strip():
                raise ValueError("Health values must be finite and have an explicit metric and unit")
            sid=row.get("id") or digest(row)
            yield {"source":source,"source_id":sid,"occurred_at":when,"kind":"health_observation",
                   "text":f"{row['metric']}: {row['value']} {row['unit']} at {when}; device: {row.get('device','unspecified')}",
                   "metadata":{"measurement":row,"value_decimal":str(value),"interpretation":"raw measurement; no medical inference"}}


def jsonl(path):
    with Path(path).open(encoding="utf-8") as stream:
        for line in stream:
            if line.strip(): yield json.loads(line)



def typed_health(client, record, record_id, subject_id):
    """Map an explicit subject's imported measurement into the typed store.

    The raw evidence is already committed. A typed failure is resumable using
    its canonical record ID; it never guesses a metric, unit or person.
    """
    measurement=record['metadata']['measurement']
    return client.call('/v1/measurement',{'key':'health/'+record_id+'/'+subject_id,
        'subject_id':subject_id,'metric':measurement['metric'],'value':float(measurement['value']),
        'unit':measurement['unit'],'measured_at':measurement['timestamp'],
        'evidence':[{'record_id':record_id,'quote':record['text']}]})

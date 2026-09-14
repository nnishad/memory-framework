"""Candidate relevance heuristics; never proof of an answer or absence."""
import math
import re
import unicodedata

STOP=frozenset('a an the is are was were be been being do does did have has had i me my mine we our you your it its this that these those what which who whom whose where when why how can could would should please tell about of for to in on at from with and or as now number type private much'.split())
def terms(text):
    return {t for t in re.findall(r'[^\W_]+',unicodedata.normalize('NFKC',text).casefold()) if t not in STOP and not t.isdigit()}

class RelevanceGate:
    def __init__(self,config=None):
        config={} if config is None else config
        if not isinstance(config,dict) or set(config)-{'semantic_minimum'}:raise ValueError('Invalid relevance configuration')
        self.minimum=config.get('semantic_minimum',0.5)
        if isinstance(self.minimum,bool) or not isinstance(self.minimum,(int,float)) or not math.isfinite(self.minimum) or not 0<=self.minimum<=1:raise ValueError('semantic_minimum must be finite and between 0 and 1')
    def assess(self,queries,text,similarity=None):
        document=terms(text)
        lexical=any(bool(terms(q)&document) for q in queries)
        semantic=isinstance(similarity,(int,float)) and math.isfinite(similarity) and similarity>=self.minimum
        return {'accepted':lexical or semantic,'lexical_anchors':lexical,'semantic_similarity':similarity,'semantic_minimum':self.minimum,'method':'content_anchors_or_semantic_floor','answer_verified':False}

import pandas as pd
from pathlib import Path
import spacy
from spacy.matcher import Matcher
import yaml
from spacy.util import filter_spans
from spacy.tokens import Doc, DocBin, Span


from journals.config import DATA_DIR, INPUT_DATA_DIR
from journals.utils import clean_text


Span.set_extension("abbrv", default=None, force=True)


class AbbreviationMatcher(object):

    nlp = spacy.load("en_core_web_sm")

    def __init__(self):
        self.matcher = self._init_matcher()

    @staticmethod
    def _get_pattern_for_term(term):

        re_num_or_numerals = r"(?i)^([0-9\-]+|[IVXLCDM]+|[A-Z])$"

        patterns = [
                [
                    {"LOWER": term.lower()},                  
                    {"IS_PUNCT": True, "ORTH": ".", "OP": "?"}, 
                    {"IS_PUNCT": True, "ORTH": ",", "OP": "?"}, 
                    {"LOWER": {'REGEX': re_num_or_numerals}},
                ],
                [
                    {"LOWER": term.lower()},                  
                    {"IS_PUNCT": True, "ORTH": ".", "OP": "?"}, 
                    {"IS_PUNCT": True, "ORTH": ",", "OP": "?"}, 
                    {"LOWER": {'REGEX': re_num_or_numerals}},
                    {"LOWER": '-'},  
                    {"LOWER": {'REGEX': re_num_or_numerals},  "OP": "?"},
                ],        
            ]  
        
        return patterns

    def _init_matcher(self):
        matcher = Matcher(self.nlp.vocab)

        with (INPUT_DATA_DIR / 'abbrv.yml').open('r') as f:
            content = f.read()
            abbrv_data = yaml.safe_load(content)        

        for k, v in abbrv_data.items():
            abbrvs = v.get('abbrv', [])
            matcher.add(k, self._get_pattern_for_term(k))
            for abbrv in abbrvs:
                matcher.add(k, self._get_pattern_for_term(abbrv))        

        return matcher

    def parse(self, text):
        text = clean_text(text)
        doc = self.nlp(text)
        matches = self.matcher(doc)
        title = self._get_title(doc, matches)
        matches = self._process_matches(doc, matches)        
        return matches, title

    def _process_matches(self, doc, matches):    

        matched_spans = []
        data = {}            

        for match_id, start, end in matches:
            matched_span = doc[start:end]
            matched_span._.abbrv = doc.vocab.strings[match_id]
            matched_spans.append(matched_span)

        matched_spans = filter_spans(matched_spans)     

        for matched_span in matched_spans:    
            expanded_span = self._expand_span(doc, matched_span)

            # Is this conjoined - e.g. Bd.10 - and matched by the regex
            if '.' in matched_span[0].text:     
                parts = expanded_span.text.split('.')
                value = parts[-1]
            elif expanded_span[1].text == '-':
                value = expanded_span.text
            elif expanded_span[1].text == '.':
                value = expanded_span[2:].text
            else:
                value = expanded_span[1:].text


            if data.get(matched_span._.abbrv):
                stripped_value = value.strip()
                if not stripped_value in data[matched_span._.abbrv].split(','):            
                    data[matched_span._.abbrv] += f', {value.strip()}'
            else:
                data[matched_span._.abbrv] = value.strip()        

        return data
    
    @staticmethod
    def _expand_span(doc, matched_span):

        has_comma = False
        has_num = False
        i = matched_span.end
        while i < len(doc):       
            # try:
            token = doc[i]

            try:
                next_token = doc[i+1]
            except IndexError:
                break
            else:
                if next_token.text == '-':
                    break
            
            if token.text == ",":
                has_comma = True
            elif token.like_num:
                has_num = True
            else:
                break
                
            i+=1

        # Ensure we have expanded to include 
        if has_comma and has_num:
            return doc[matched_span.start: i]    
        else:
            return matched_span        

    @staticmethod
    def _get_title(doc, matches):

        if ';' in doc.text:
            title = doc.text.split(';')[0]
            return title.strip()
        elif matches:
            first_match = min([start for _, start, end in matches])
            title_span = doc[0:first_match]
            return title_span.text
        else:
            return doc.text


if __name__ == "__main__":
    matcher = AbbreviationMatcher()
    # text = 'Number 302-307 (1996)'
    text = "Bollettino dei Musei e degli Istituti Biologici dell'Università di Genova ; vol. 68 (2003-2004)"
    matches = matcher.parse(text)
    print(matches)








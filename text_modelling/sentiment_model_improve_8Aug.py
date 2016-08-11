__author__ = 'joswin'

from nltk import sent_tokenize, word_tokenize, pos_tag, BigramTagger, ChunkParserI
from nltk.chunk.util import conlltags2tree,tree2conlltags

class BigramChunker(ChunkParserI):
    def __init__(self, train_sentences):
        train_data = [[(t, c) for w, t, c in tree2conlltags(sent)]
                      for sent in train_sentences]
        # Create a bigram tagger and use the supplied training data to create the model
        self.tagger = BigramTagger(train_data)

    # Extracts chunks from a sentence using the conll tree format. The incoming sentence needs to be
    # tokenized and annotated with POS tags
    def parse(self, sentence):
        pos_tags = [pos for (word, pos) in sentence]
        tagged_pos_tags = self.tagger.tag(pos_tags)
        chunk_tags = [chunk_tag for (pos, chunk_tag) in tagged_pos_tags]
        conll_tags = [(word, pos, chunk_tag) for ((word, pos), chunk_tag) in zip(sentence, chunk_tags)]
        return conlltags2tree(conll_tags)


class TextChunker:
    def __init__(self,train_sents):
        # loading the training data for the chunker
        self.chunker = BigramChunker(train_sents)
    # Given a document of sentences calculate the containing chunks for each sentence
    def chunk_text(self, rawtext):
        if self.chunker is None:
            raise Exception("Text chunker needs to be trained before it can be used.")
        sentences = sent_tokenize(rawtext.lower())  # NLTK default sentence segmenter
        tokenized = [word_tokenize(sent) for sent in sentences]  # NLTK word tokenizer
        postagged = [pos_tag(tokens) for tokens in tokenized]  # NLTK POS tagger
        for tagged in postagged:
            for chunk in self._extract_chunks(self.chunker.parse(tagged), exclude=["NP", ".", ":", "(", ")"]):
                if len(chunk) >= 2:
                    yield chunk
    def _token_of(self, tree):
        return tree[0]
    def _tag_of(self, tree):
        return tree[1]
    # The chunker will produce a parse tree. We need to analyse the parse tree and
    # extract and combine the tags we want.
    def _extract_chunks(self, tree, exclude):
        def traverse(tree):
            try:
                # Let's check if we are at a leaf node containing a token
                tree.label()
            except AttributeError:
                # We want to exclude all POS tags in `exclude` and furhtermore we want to ignore special characters.
                # The POS tag of a special character is equal to the character. The only other token for which this is
                # true is `to` so we need to make sure to exclude everything but `to`.
                if self._tag_of(tree) in exclude \
                        or self._token_of(tree) in exclude \
                        or (self._token_of(tree) != "to" and self._token_of(tree) == self._tag_of(tree)):
                    return []
                else:
                    # return the token of the node
                    return [self._token_of(tree)]
            else:
                node = tree.label()
                if node in exclude:
                    return []
                else:
                    return [word for child in tree for word in traverse(child)]
        for child in tree:
            traversed = traverse(child)
            if len(traversed) > 0:
                # chunks get conected again using whitespaces
                yield " ".join(traversed)


import nltk
nltk.help.upenn_tagset('RB')

grammar = """MEDIA: {<DT>?<JJ>*<NN.*>+}
           RELATION: {<V.*>}
                     {<DT>?<JJ>*<NN.*>+}
           ENTITY: {<NN.*>}"""
grammar = r"""
  NP: {<DT|JJ|NN.*>+}          # Chunk sequences of DT, JJ, NN
  PP: {<IN><NP>}               # Chunk prepositions followed by NP
  VP: {<VB.*><NP|PP|CLAUSE>+$} # Chunk verbs and their arguments
  CLAUSE: {<NP><VP>}           # Chunk NP, VP
  """
grammar = r"""
    NP: {<DT|JJ|NN.*>+}          # Chunk sequences of DT, JJ, NN
    PP: {<IN><NP>}               # Chunk prepositions followed by NP
    VP: {<VB.*><NP|PP>+} # Chunk verbs and their arguments
    CLAUSE: {<NP><VP>}           # Chunk NP, VP
    """

########################################################################
################# final grammar #####################################
##################################################################

# not in the market  -> not working, need to catch

grammar = r"""
    P2: {<RB>*<VB.*|JJ>}         # not interested, no need
    P3: {<PR.+>*<RP|IN>}        #me off
    P4: {<PR.+><NN.*>}          # your list
    P7: {<P2><P3|NN.*|PR.+>}         #Take me off, good idea,Tell me, right person
    P8: {<P7><CC>}         #good idea but
    """
# P1: {<IN><NP|PR.+>}               # Chunk for me, for us,
# P5: {<P2><P4>}     # take me off
#    P6: {<VB.*><PR.+>*}        #Take me, remove me
#     P9: {<RB>*<IN>*<DT>*<NN.*|PR.+|DT>} #not in the market


cp = nltk.RegexpParser(grammar)

print(cp.parse(nltk.pos_tag(nltk.word_tokenize('Take me off your list'))))
print(cp.parse(nltk.pos_tag(nltk.word_tokenize('Thanks but we are not interested'))))
print(cp.parse(nltk.pos_tag(nltk.word_tokenize('How does this work?'))))
print(cp.parse(nltk.pos_tag(nltk.word_tokenize('I am not sure if I understand what you have to offer.'))))
print(cp.parse(nltk.pos_tag(nltk.word_tokenize('I am not responsible for tech.'))))
print(cp.parse(nltk.pos_tag(nltk.word_tokenize('This is not interesting for me'))))
print(cp.parse(nltk.pos_tag(nltk.word_tokenize('This is interesting for me'))))
print(cp.parse(nltk.pos_tag(nltk.word_tokenize('Tell me more about your product'))))
print(cp.parse(nltk.pos_tag(nltk.word_tokenize('This seems a good idea but we are not looking for these kinds of services as of now.'))))
print(cp.parse(nltk.pos_tag(nltk.word_tokenize('This seems a good idea, but we are not looking for these kinds of services as of now.')))) # this is not working(need to remove commas?
print(cp.parse(nltk.pos_tag(nltk.word_tokenize('Tell me about your offering.'))))
print(cp.parse(nltk.pos_tag(nltk.word_tokenize("No. We are not in the market for this."))))




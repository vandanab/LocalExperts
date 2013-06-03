'''
Created on May 7, 2013
@author: vandana
Extending lucene classes
'''
from lucene import PythonCharTokenizer, LowerCaseFilter, \
                    PythonAnalyzer

class UnderscoreSeparatorTokenizer(PythonCharTokenizer):
  def __init__(self, input):
    PythonCharTokenizer.__init__(self, input)
  
  def isTokenChar(self, c):
    return c != "_"

class UnderscoreSeparatorAnalyzer(PythonAnalyzer):
  def __init__(self, version):
    PythonAnalyzer.__init__(self, version)

  def tokenStream(self, fieldName, reader):
    tokenizer = UnderscoreSeparatorTokenizer(reader)
    tokenStream = LowerCaseFilter(tokenizer)
    return tokenStream

"""

class ListTokenStream(PythonTokenStream):
  def __init__(self, terms):
    PythonTokenStream.__init__(self)
    self.terms = iter(terms)
    self.addAttribute(TermAttribute.class_)
  def incrementToken(self):
    for term in self.terms:
      self.getAttribute(TermAttribute.class_).setTermBuffer(term)
      return True
    return False

class TokenFilter(lucene.PythonTokenFilter, TokenStream):
  def __init__(self, input):
    PythonTokenFilter.__init__(self, input)
    self.terms = iter(input)
  def incrementToken(self):
    "Advance to next token and return whether the stream is not empty."
    result = self.input.incrementToken()
    return result
"""


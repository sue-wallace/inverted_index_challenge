# Databricks notebook source
# MAGIC %run ./inverted_index

# COMMAND ----------

from unittest import TestCase 

# COMMAND ----------

test_data_path = '/dbfs/mnt/sue_wallace/sue_inverted_test/'
test_write_path = '/dbfs/mnt/sue_wallace/sue_test_csv_write/test._write.csv'

# COMMAND ----------

class TestInvertedIndex(TestCase):
  """Testing the createDictionary function is working as expected
     @param: expected_df - the data that is expected once the function has been passed over txt files
     @param: given_df - the data that the function creates"""
  
  def __init__(self):
    self._expected_df = None 
    self._given_df = None 
    self._blacklist = None 
    self._punctuation = None 
    
    
  def setUp(self):
    """  """
    self._expected_df = _expected_df = {'here': {'Document': ['test1.txt', 'test2.txt']}, 'is': {'Document': ['test1.txt', 'test2.txt']}, 'some': {'Document': ['test1.txt', 'test2.txt']}, 'test': {'Document': ['test1.txt', 'test2.txt']}, 'data': {'Document': ['test1.txt', 'test2.txt']}, 'the': {'Document': ['test1.txt', 'test2.txt']}, 'should': {'Document': ['test1.txt']}, 'not': {'Document': ['test1.txt']}, 'appear': {'Document': ['test1.txt', 'test2.txt']}, 'in': {'Document': ['test1.txt', 'test2.txt']}, 'teh': {'Document': ['test1.txt']}, 'dictionary': {'Document': ['test1.txt', 'test2.txt']}, 'there': {'Document': ['test1.txt']}, 'be': {'Document': ['test1.txt']}, 'any': {'Document': ['test1.txt']}, 'upper': {'Document': ['test1.txt']}, 'case': {'Document': ['test1.txt']}, 'or': {'Document': ['test1.txt']}, 'punctuation': {'Document': ['test1.txt']}, 'symbols': {'Document': ['test1.txt']}, 'like': {'Document': ['test1.txt']}, 'this': {'Document': ['test1.txt']}, 'hello': {'Document': ['test1.txt']}, 'all': {'Document': ['test1.txt']}, 'lower': {'Document': ['test1.txt']}, 'more': {'Document': ['test2.txt']}, 'words': {'Document': ['test2.txt']}, 'duplicated': {'Document': ['test2.txt']}, 'across': {'Document': ['test2.txt']}, 'both': {'Document': ['test2.txt']}, 'txt': {'Document': ['test2.txt']}, 'file': {'Document': ['test2.txt']}, 'whold': {'Document': ['test2.txt']}, 'just': {'Document': ['test2.txt']}, 'once': {'Document': ['test2.txt']}}
    self._given_df = createDictionary()


    return self._expected_df, self._given_df 
  
  def blacklist_checker(self):
    
    """ Fucntion to check whether expected words and punctuation
    have been omitted from the dictionary
    """
    
    blacklist = ['this!', 'data!', 'dictionary!']

    for char in self._given_df :
      assert char not in blacklist, ('FAIL: function has failed to omit a punctuation mark from the dictionary)')
  
  def expected_records(self): #check that the two returned dfs match, if not then the fucntion is failing
    
    function_index = self._given_df
    expected = self._expected_df
    
    assert function_index == expected, "FAIL: The dictionary from the inverted index function does not match the expected index"

# COMMAND ----------

test = TestInvertedIndex()
test.setUp()
test.blacklist_checker()
test.expected_records()

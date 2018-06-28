import pyspark as ps
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from string import punctuation

try:
    sc = ps.SparkContext("local", "Simple App")
    sqlContext = SQLContext(sc)
    print("Just created a SparkContext")
except ValueError:
    warnings.warn("SparkContext already exists in this scope")
spark = SparkSession(sc)


train_path='/home/hadoop/spark/lab3.csv'
train_rdd = sc.textFile(train_path)

def tolower(lines):
      lines = lines.lower()
      return lines




import re
def remove_numbers(s):
	s = re.sub('^[0-9]+', '', s)
	return(s)

train_rdd = train_rdd.map(tolower).map(remove_numbers)


def strip_punctuation(s):
        	return ''.join(c for c in s if c not in punctuation)

def parseTrain(rdd):
 
    # extract data header (first row)
    header = rdd.first()
    print(header)
    # remove header
    body = rdd.filter(lambda r: r!=header)
    def parseRow(row):

        # a function to parse each text row into
        # data format
        # remove double quote, split the text row by comma
        stopwords = ['ourselves', 'hers', 'between', 'yourself', 'but', 'again', 'there', 'about', 'once', 'during', 'out', 'very', 'having', 'with', 'they', 'own', 'an', 'be',
     'some', 'for', 'do', 'its', 'yours', 'such', 'into', 'of', 'most', 'itself', 'other', 'off', 'is', 'am', 'or', 'who', 'as', 'from', 'him', 'each', 'the', 'themselves',
      'below', 'are', 'we', 'these', 'your', 'his', 'through', 'don', 'nor', 'me', 'were', 'her', 'more', 'himself', 'this', 'down', 'should', 'our', 'their', 'while', 
      'above', 'both', 'up', 'to', 'ours', 'had', 'she', 'all', 'no', 'when', 'at', 'any', 'before', 'them', 'same', 'and', 'been', 'have', 'in', 'will', 'on', 'does', 'yourselves',
       'then', 'that', 'because', 'what', 'over', 'why', 'so', 'can', 'did', 'not', 'now', 'under', 'he', 'you', 'herself', 'has', 'just', 'where', 'too', 'only', 'myself', 'which',
        'those', 'after', 'few', 'whom', 'being', 'if', 'theirs', 'my', 'against', 'a', 'by', 'doing', 'it', 'how', 'further', 'was', 'here', 'than']
        row_list = row.replace('advertisementsupported','').split(",")
        new_rowlist = [0,0]
        new_rowlist[0] = row_list[0]
        new_rowlist[1] = " ".join(row_list[1:])
        for i in stopwords:
        	new_rowlist[1] = new_rowlist[1].replace(i, " ")
        new_rowlist[1] = strip_punctuation(str(new_rowlist[1]))
        new_rowlist[1] = re.sub("\s\s+" , " ", str(new_rowlist[1]))
        

        # convert python list to tuple, which is
        # compatible with pyspark data structure
        row_tuple = tuple(new_rowlist)
        return row_tuple
 
    rdd_parsed = body.map(parseRow)
 
    colnames = header.split(",")
 
    return rdd_parsed.toDF(colnames)

df = parseTrain(train_rdd)



import numpy as np
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.feature import NGram

# using term frequency
tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashtf = HashingTF(numFeatures=2**10, inputCol="words", outputCol='features')
label_stringIdx = StringIndexer(inputCol = "tar", outputCol = "label")
pipeline = Pipeline(stages=[tokenizer, hashtf, label_stringIdx])


#using tf-idf
# tokenizer = Tokenizer(inputCol="text", outputCol="words")
# hashtf = HashingTF(numFeatures=2**10, inputCol="words", outputCol='tf')
# idf = IDF(inputCol='tf', outputCol="features") #minDocFreq: remove sparse terms
# label_stringIdx = StringIndexer(inputCol = "tar", outputCol = "label")
# pipeline = Pipeline(stages=[tokenizer, hashtf, idf, label_stringIdx])


pipelineFit = pipeline.fit(df)
train_df = pipelineFit.transform(df)

(train_set, test_set, final_testset) = train_df.randomSplit([0.8, 0.1, 0.1], seed = 1235)

#Logistic Regression
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
lr = LogisticRegression(maxIter=250)
lrModel = lr.fit(train_set)
#predictions on training
predictions = lrModel.transform(train_set)
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
train_logistic = evaluator.evaluate(predictions)
#predictions on testing
predictions = lrModel.transform(test_set)
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
test_logistic = evaluator.evaluate(predictions)




#Naive Bayes
from pyspark.ml.classification import NaiveBayes
nb = NaiveBayes(smoothing=10e-10)
model = nb.fit(train_set)
#predictions on training
predictions = model.transform(train_set)
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
train_nb = evaluator.evaluate(predictions)
#predictions on testing.
predictions = model.transform(test_set)
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
test_nb = evaluator.evaluate(predictions)




#RandomForest
from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(labelCol="label", \
                            featuresCol="features", \
                            numTrees = 100, \
                            maxDepth = 5)
rfModel = rf.fit(train_set)
#predictions on training
predictions = rfModel.transform(train_set)
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
train_rf = evaluator.evaluate(predictions)
#predictions on testing.
predictions = rfModel.transform(test_set)
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
test_rf = evaluator.evaluate(predictions)


#predictions on newdata
predictions = lrModel.transform(final_testset)
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
finaltest_logistic = evaluator.evaluate(predictions)

predictions = model.transform(final_testset)
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
finaltest_nb = evaluator.evaluate(predictions)

predictions = rfModel.transform(final_testset)
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
finaltest_rf = evaluator.evaluate(predictions)


print("\n\n\n")
print("Predictions on Trainingset Results: ")
print("Logistic Regression Acc:", train_logistic)
print("Naive Bayes Acc:", train_nb)
print("Random Forest Acc:", train_rf)
print("\n\n\n")
print("Predictions on Testingset Results: ")
print("Logistic Regression Acc:", test_logistic)
print("Naive Bayes Acc:", test_nb)
print("Random Forest Acc:", test_rf)


print("\n\n\n")
print("Final Test Results (New Data): ")
print("Logistic Regression Acc:", finaltest_logistic)
print("Naive Bayes Acc:", finaltest_nb)
print("Random Forest Acc:", finaltest_rf)
print("\n\n\n")

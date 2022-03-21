# Basics
Mapper: to handle mapping.

Reducer.

Driver.

## MapReaduce Job
Input<key1, value1> => map() in Mapper=> list(<key2 ,value2>) => intermediate process => <key2, list(value2)> => reduce()in Reducer => list(<key3, value3>) output 

## DataTypes
1. BooleanWritable
2. ByteWritable
3. IntWritable
4. LongWritable
5. DoubleWritable
6. FloatWritable
7. VLongWritable : variable length
8. VIntWritable : variable length

9. Text : max size 2 gb
10. NullWritable

---------------------
Hadoop uses 128mb as the size of blocs, to reduce the number of requests when handling massive data.

Number of mappers == number of splits == number of blocs, 

By default the number of reducers is 1, if we need more we should add them explicitly.

## WordCount Process Example 

### Records 

hello there
hello again1
there is a problem again

each record is a line of input
### split (input)
(0, hello there)

(12, hello again) # 'hello there' got 10 chars + new line char => 11 

(25, no problem again)

### mapping

(0, hello there) 			| => map() => | (hello,1), (there,1)  
        
(12, hello again)			| => map() => | (hello, 1),(again, 1)   
     
(25, no problem again) | => map() => | (no, 1), (problem, 1), (again, 1)

(LongWritable, Text) => map() => (Text, IntWritable)

### sorting data by key by MapReduce
(again, 1)

(again, 1)

(hello, 1)

(hello, 1)

(no, 1)

(problem, 1)

(there, 1)

(there, 1)

## grouping accurence by key
(again, (1,1))

(hello, (1,1))

(no, (1))

(problem, (1))

(there, (1,1))

-> to be passed to Reducer to apply logic on it

## Reducer logic (sum of values)

(again, 2)

(hello, 2)

(no, 1)

(problem, 1)

(there, 2)

<Text, IntWritable> => reduce() => <Text, IntWritable>


-----------------------------

Workflow of MapReduce consists of 5 steps:

1. Splitting – The splitting parameter can be anything, e.g. splitting by space, comma, semicolon, or even by a new line (‘\n’).

2. Mapping – as explained above.

3. Intermediate splitting – the entire process in parallel on different clusters. In order to group them in “Reduce Phase” the similar KEY data should be on the same cluster.

4. Reduce – it is nothing but mostly group by phase.

5. Combining – The last phase where all the data (individual result set from each cluster) is combined together to form a result.

![Work Flow of the Program](https://dz2cdn1.dzone.com/storage/temp/1329325-111.png)

 
 ## Executing Job;
 1. export the application as jar.
 2. lunch the application : 


``` bash
hadoop jar wordCount_m1.jar com.elaamiri.wordCount.App /wordsCountFile /wordCountResult2
 
```


> wordCount_m1.jar : the name of jar file

> com.elaamiri.wordCount.App :  the main class

> /wordsCountFile : the path of the data file in hdfs

> /wordCountResult2 : the path in hdfs where to put the output of the job


 












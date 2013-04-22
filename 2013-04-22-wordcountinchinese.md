---
layout:post
title:hadoop 实现中文的wordcount
tags:
-Ubuntu
-Hadoop
-MapReduce
categories:
-linux
-Hadoop
UUID:20130422
---
  近来有一霸气理工男算出了宋词的100个高频词，然后开发了一个作词的神器。闲着无聊就用mapreduce实现了中文的wordcount。
和mapreduce入门的wordcount比，中文的wordcount需要加上中文分词。网上分词器有很多，我们选取IKAnalyzer对需要统计的文档
做中文分词。
IKAnalyzer下载：http://code.google.com/p/ik-analyzer/
导入IKAnalyzer.cfg.xml，stopword.dic，IKAnalyzer.jar，lucene-core.jar(需要lucene的支持)

代码如下：

package org.daopenghu.HdaoopMapReduce.examples;


import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;


public class WordCountInChinsese {

	/**
	 * @param args
	 */
	public static class TokenizerMapper extends
	Mapper<Object, Text, Text, IntWritable> {

private final static IntWritable one = new IntWritable(1);
private Text word = new Text();

public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
	//create analyzer object
	Analyzer anal=new IKAnalyzer(true);
	StringReader reader=new StringReader(value.toString());
	
	//analyze
	TokenStream ts=anal.tokenStream("", reader);
	CharTermAttribute term=ts.getAttribute(CharTermAttribute.class);
	//traverse
	while(ts.incrementToken()){
		System.out.print(term.toString()+"|");
		word.set(term.toString());
		context.write(word, one);
	}
	reader.close();
	anal.close();
	System.out.println();
}
}

public static class IntSumReducer extends
	Reducer<Text, IntWritable, Text, IntWritable> {
private IntWritable result = new IntWritable();

//word count reducer 
public void reduce(Text key, Iterable<IntWritable> values,
		Context context) throws IOException, InterruptedException {
	int sum = 0;
	for (IntWritable val : values) {
		sum += val.get();
	}
	result.set(sum);
	context.write(key, result);
}
}

public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();
if (otherArgs.length != 2) {
	System.err.println("Usage: wordcount <in> <out>");
	System.exit(2);
}
Job job = new Job(conf, "word count");
job.setJarByClass(WordCount.class);
job.setMapperClass(TokenizerMapper.class);
job.setCombinerClass(IntSumReducer.class);
job.setReducerClass(IntSumReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}

package main

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path

// Author: Habiba and Neelesh
object Demo {
    def main(args: Array[String]) {
        println("Demo: startup")

        // Make a job
        val job = Job.getInstance()
        job.setJarByClass(Demo.getClass)
        job.setJobName("Demo")

        // Set classes mapper, reducer, input, output.
        job.setMapperClass(classOf[DemoMapper])
	if(args(2).compareTo("0") > 0) { 
		job.setReducerClass(classOf[MeanReducer])
	} else if(args(2).compareTo("1") > 0) {
		job.setReducerClass(classOf[MedianReducer])
	} else {
		job.setReducerClass(classOf[FastReducer])
	}
	
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[Text])

        FileInputFormat.addInputPath(job,  new Path(args(0)))
        FileOutputFormat.setOutputPath(job, new Path(args(1)))

        // Actually run the thing.
        job.waitForCompletion(true)
    }
}


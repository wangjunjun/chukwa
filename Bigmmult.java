import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class Bigmmult {

	public static final String CONTROL_I="\u0009";
	public static final int MATRIX_I = 4;
	public static final int MATRIX_J = 3;
	public static final int MATRIX_K = 2;
	
//	public static String makeKey(String[] tokens,String separator){
//		StringBuffer sb = new StringBuffer();
//		boolean isFirst = true;
//		for(String token : tokens){
//			if(isFirst)
//				isFirst = false;
//			else
//				sb.append(separator);
//			sb.append(token);
//		}
//		return sb.toString();
//	}
	
	public static class MapClass extends Mapper<Object, Text, Text, Text>{
//		public static HashMap<String , Double> features = new HashMap<String, Double>();  
		
		public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
			System.out.println("Start get pathname");
			String pathName = ((FileSplit)context.getInputSplit()).getPath().toString();
		     System.out.println(pathName);
			if(pathName.contains("a.txt")){
				String line = value.toString();
				if(line == null || line.equals(" ")) return;
				
				System.out.println("A+"+line);
				String[] values = line.split(CONTROL_I);
				System.out.println(values.length);
				if(values.length < 3)return;
				
				String rowindex = values[0];
				String colindex = values[1];
				String elevalue = values[2];
				System.out.println(elevalue);
				for(int i = 1; i<= MATRIX_K;i++)
				{
					System.out.println("A:"+rowindex+CONTROL_I+i);
					context.write(new Text(rowindex+CONTROL_I+i), new Text("a#"+colindex+"#"+elevalue));
					
				}
			}
			
			if(pathName.contains("b.txt")){
				String line = value.toString();
				if(line== null || line.equals(" "))return;
				System.out.println("B+"+line);
				String values[] = line.split(CONTROL_I);
				if(values.length<3)return;
				String rowindex = values[0];
				String colindex = values[1];
				String eleindex = values[2];
				
				for(int i=1;i<= MATRIX_I;i++)
				{
					System.out.println("B:"+i+CONTROL_I+colindex);
					context.write(new Text(i+CONTROL_I+colindex), new Text("b#"+rowindex+"#"+eleindex));
				}
			}
		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int[] valA = new int[MATRIX_J];
			int[] valB = new int[MATRIX_J];
			
			int i;
			for(i=0;i<MATRIX_J;i++)
			{
				valA[i] = 0;
				valB[i] = 0;
			}
			
			Iterator<Text> ite = values.iterator();
			//System.out.println(values.toString());
			while(ite.hasNext())
			{
				//System.out.println(ite.next().toString());
				String value = ite.next().toString();
				if(value.startsWith("a#"))
				{
//					System.out.println(value);
					StringTokenizer token = new StringTokenizer(value ,"#");
					String[] temp = new String[3];
					int k = 0;
					while(token.hasMoreTokens())
					{
						temp[k] = token.nextToken();
						k++;
					}
					valA[Integer.parseInt(temp[1])-1] = Integer.parseInt(temp[2]);
				}else if(value.startsWith("b#")){
					StringTokenizer token = new StringTokenizer(value,"#");
					String[] temp = new String[3];
					int k = 0;
					while(token.hasMoreTokens())
					{
						temp[k] = token.nextToken();
						k++;
					}
					valB[Integer.parseInt(temp[1])-1] = Integer.parseInt(temp[2]);
				}
			}
			
			int result = 0;
			for(i= 0;i< MATRIX_J;i++)
			{
				result += valA[i] * valB[i];
			}
			
			context.write(key, new Text(Integer.toString(result)));
		}
		}
	
	public static void main(String[] args)throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage:wordcount<in><out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Bigmmult");
		job.setJarByClass(Bigmmult.class);
		System.out.println("job set");
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		System.out.println("output set");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.out.println("path set ok");
		System.exit(job.waitForCompletion(true) ?0:1);
		
		
	}
}

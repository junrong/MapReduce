import java.io.IOException;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.CSVReader;


public class PlainFlightDelay {
	//global enum counters to help calculate average delay
	public enum AvgCounters{
		TotalDelay,TotalPairs;
	}
	
	public static class PlainFightMapper extends Mapper<Object, Text, Text, Text>{
		/**
		 * @author junrongyan
		 * Map Function
		 */
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
			String str = value.toString();
			//read each line as each record
			CSVReader reader = new CSVReader(new StringReader(str));
			//transfer each line to String array
			String[] line = reader.readNext();
			reader.close();
			//String format flight date yyyy-mm-hh
			String dateString = line[5].trim();
			// Departure airport
			String origin = line[11].trim();
			// Arrival airport
			String dest = line[17].trim();
			// Departure time in local hhmm
			String depTime = line[24].trim();
			// Arrival time in local hhmm
			String arrTime = line[35].trim();
			// Delay time in minutes
			String arrDelyMins = line[37].trim();
			// Cancellation status
			boolean cancelled = line[41].trim().equals("0.00")? false:true;
			// Diverted status
			boolean diverted = line[43].trim().equals("0.00")? false:true;
			
			
			Date flightDate = null;
			Date beginDate = null;
			Date endDate = null;
			DateFormat date = new SimpleDateFormat("yyyy-MM-dd");
			try {
				// FlightDate in format "Thu Jun 18 20:56:02 EDT 2009"
				 flightDate = date.parse(dateString);
				 beginDate = date.parse("2007-05-31");
				 endDate = date.parse("2008-06-01");
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			/**
			 * 
			 * @author junrongyan
			 * 
			 * The filter part:
			 * The intermediate key is: Flight Date + Destination/Origin
			 * if intermediate key is same, that means the flight date is same and the destination of Flight form ORD is 
			 * same to the origin of Flight to JFK.
			 * Thus, the intermediate value is a list of flights. They have same flight date and destination/origin is same.
			**/
			// filter out all direct flight form ORD to JFK;
			//filter out all flights whose origin is not ORD and destination is not JFK either.
			if((origin.equals("ORD"))||(dest.equals("JFK"))&&!((origin.equals("ORD"))&&(dest.equals("JFK")))){
				// filter out all flights whose flight date not in 2007/06 - 2008/05 or cancelled or diverted
				if((flightDate.before(endDate))&&(flightDate.after(beginDate))&&!cancelled&&!diverted){
					//if origin is ORD, set key as flightdate+destination
					if(origin.equals("ORD")){
						Text mapKey = new Text(dateString+dest);
						Text mapVal = new Text(origin+","+arrTime+","+""+arrDelyMins);
						context.write(mapKey, mapVal);
					}else{
						//if destination is JFK, set key as flight date + origin
						Text mapKey = new Text(dateString+origin);
						Text mapVal = new Text(dest+","+depTime+","+""+arrDelyMins); 
						context.write(mapKey, mapVal);
					}	
				}
			}	
		}
	}
	
	/**
	 * 
	 * @author junrongyan
	 * In reducer side, the reduce will perform JOIN to find out the qualifying two-leg flights pairs.
	 */
	public static class PlainFightReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException{
			List<String> FirstFlight = new ArrayList<String>();
			List<String> SecondFlight = new ArrayList<String>();
			for(Text t: values){
				String str = t.toString();
				if(str.contains("ORD")){
					// Add flight departed form ORD added to FirstFlight;
					FirstFlight.add(str);
				}else if(str.contains("JFK")){
					// Add flight fly to JFK added to SecondFlight;
					SecondFlight.add(str);
				}
			}
			/**
			 * @author junrongyan
			 * 
			 * O(n^2) time complexity algorithm to join two ArrayList.
			 * If firstFlight.arrTime < secondFlight.depTime
			 * qualify pairs(firstFlight, secondFlight)
			 */
			for(String str : FirstFlight){
				String[] arr = str.split(",");
				SimpleDateFormat parser = new SimpleDateFormat("HHmm");
				Date arrTime = null;
				try {
					arrTime = parser.parse(arr[1]);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				double firstDelyMins = (double)Float.parseFloat(arr[2]);
				for(String str2 : SecondFlight){
					String[] arr2 = str2.split(",");
					//date format
					SimpleDateFormat parser2 = new SimpleDateFormat("HHmm");
					Date depTime = null;
					try {
						depTime=parser2.parse(arr2[1]);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					double secondDelyMins =(double) Float.parseFloat(arr2[2]);
					if(arrTime.before(depTime)){
						//calculate the total delay time of this pairs.
						double totalDelay =firstDelyMins + secondDelyMins;
						context.getCounter(AvgCounters.TotalDelay).increment((long) totalDelay);
						context.getCounter(AvgCounters.TotalPairs).increment(1);
						//totalDelayMins += totalDelay;
						//++totalFlightPairs;
						//String totalDelay = Integer.toString(firstDelyMins+secondDelyMins);
						//Text reduceKey = new Text("Cal");
						//Text reduceVal = new Text(totalDelay);
						//context.write(reduceKey, reduceVal);
					}
					
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: FlightDelay <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Filght Delay Plain MapReduce");
		job.setJarByClass(PlainFlightDelay.class);
		job.setMapperClass(PlainFightMapper.class);
		job.setReducerClass(PlainFightReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job1.setNumReduceTasks(12);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// I printout the average to console and log file, no need to setOutputPath();
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		if(job.waitForCompletion(true)){
			// get the TotalDelay and TotalPairs form job counters
			long totalDelayMins = job.getCounters().findCounter(AvgCounters.TotalDelay).getValue();
			long totalFlightPairs = job.getCounters().findCounter(AvgCounters.TotalPairs).getValue();
		System.out.println("The average flight delay is " + (float)totalDelayMins/(float)totalFlightPairs);
		System.out.println("The total delayMinutes is " + (float)totalDelayMins);
		System.out.println("The total flight pairs are " + totalFlightPairs);
		System.exit(job.waitForCompletion(true)?0:1);
		}

	}
}

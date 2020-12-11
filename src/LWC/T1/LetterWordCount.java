package LWC.T1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
// ykkim addition below
import java.util.LinkedHashMap;  
import java.util.LinkedList; 
import java.util.Map; 
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

// k8s APIs
import io.fabric8.kubernetes.api.KubernetesClient;
import io.fabric8.kubernetes.api.KubernetesFactory;

public class LetterWordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    static enum CountersEnum { INPUT_WORDS }

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private boolean caseSensitive;
    private Set<String> patternsToSkip = new HashSet<String>();

    private Configuration conf;
    private BufferedReader fis;

    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
      conf = context.getConfiguration();
      caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
      if (conf.getBoolean("wordcount.skip.patterns", false)) {
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
        for (URI patternsURI : patternsURIs) {
            Path patternsPath = new Path(patternsURI.getPath());
            String patternsFileName = patternsPath.getName().toString();
            parseSkipFile(patternsFileName);
                  }
              }
          }

     private void parseSkipFile(String fileName) {
        try {
          fis = new BufferedReader(new FileReader(fileName));
          String pattern = null;
          while ((pattern = fis.readLine()) != null) {
            patternsToSkip.add(pattern);
                      }
        } catch (IOException ioe) {
          System.err.println("Caught exception while parsing the cached file '"
              + StringUtils.stringifyException(ioe));
                 }
            }

      @Override
      public void map(Object key, Text value, Context context
                      ) throws IOException, InterruptedException {
    	   String line = value.toString().toLowerCase();
        for (String pattern : patternsToSkip) {
          line = line.replaceAll(pattern, "");
                  }
        StringTokenizer itr = new StringTokenizer(line);
        String token;
        while (itr.hasMoreTokens()) {
          word.set(token=itr.nextToken());
          context.write(word, one);
          Counter counter = context.getCounter(CountersEnum.class.getName(),
              CountersEnum.INPUT_WORDS.toString());
          counter.increment(1);

          try {
        	  int cp = 0;
        	  char ct; 
        	  while ( Character.isAlphabetic (ct = token.charAt(cp)) ) {
        		  word.set("______" + ct);
        		  context.write(word,  one);
        		  cp++;
          			  	}      	     
          } catch (Exception ignore) {}
        		}
      		}
    }

    public static class IntSumReducer
         extends Reducer<Text,IntWritable,Text,IntWritable> {
      private IntWritable result = new IntWritable();
   
      public void reduce(Text key, Iterable<IntWritable> values,
                         Context context
                         ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values)  sum += val.get();
        result.set(sum);
        context.write(key, result);
      		}
      
    	}

    
        	private static void postProcess(Configuration conf, Path file_path) throws Exception {   	      

      	      String file_name = file_path.toString() + "/part-r-00000";
      	    
    	      Path inFile = new Path (file_name);   
    	      FileSystem fs = FileSystem.get(conf);    	     
    	      FSDataInputStream f_is = fs.open(inFile);

    	      BufferedReader br = new BufferedReader (new InputStreamReader (f_is) );
    	      
    	      List<WordCnt> words = new ArrayList<WordCnt>();
    	      List<CharCnt> characters = new ArrayList<CharCnt>();
    	      
    	      int nRecord=0;    
    	      int nWords=0;
    	      int nCharacters=0;
    	      int tWords=0;
    	      
    	      try {
    	        String line;
    	        while ((line = br.readLine()) != null) {
    	        	 String[] tokens = line.split("\t");
  	          
    	          if ( (tokens[0].length() == 7) && ("______".equals( tokens[0].substring(0,6) ) ) ) { 
    	        	  characters.add( new CharCnt(tokens[0].charAt(6), Integer.parseInt(tokens[1]) ));
    	        	  nCharacters++;
    	          } else {
    	        	  int freq = Integer.parseInt(tokens[1]);
    	        	  words.add((new WordCnt(tokens[0], freq )));
    	        	  nWords++;
    	        	  tWords += freq;
    	          			      }
    	          nRecord++;
    	        			}
    	      } finally {
    	    	  	f_is.close();
    	        		}
    	      words.sort(new Comparator<WordCnt>() {
    	    	  @Override
    	    	  public int compare(WordCnt arg0, WordCnt arg1) {
    	    		if ( arg0.count == arg1.count )  return 0;
    	    		else if (arg0.count < arg1.count) return 1;
    	    		else return -1;
    	    	  			}
    	      			});
    	      characters.sort(new Comparator<CharCnt>() {
    	    	  @Override
    	    	  public int compare(CharCnt arg0, CharCnt arg1) {
    	    		if ( arg0.count == arg1.count )  return 0;
    	    		else if (arg0.count < arg1.count) return 1;
    	    		else return -1;
    	    	  			}
    	      			});
    	       	      
    	      // calculate the number of record for the highest 5%, common 47.5~52.5%, the lowest 5%
    	      int nPrint = (int) Math.ceil(nWords * 0.05);
    	      int nStart = ( nWords/2 - (int) Math.ceil(nWords * 0.025) );
    	      int nEnd = ( nWords/2 + (int) Math.ceil(nWords * 0.025) );
 // Summary output
    	      System.out.println("------------------------------------------------------------");
    	      System.out.println("Total number of words : " + tWords);
    	      System.out.println("Total number of distinct words : " + nWords);
    	      System.out.println("Popular threahold words : " + nPrint);
    	      System.out.println("Common threahold l words : " + (nStart+1));
    	      System.out.println("Common threahold u words : " + nEnd);
    	      System.out.println("Rare threahold words : " + (nWords - nPrint + 1));
    	      System.out.println("------------------------------------------------------------");	      
    	      
    	      System.out.println("Popular words");
    	      System.out.println("+------+-------------------+-----------+");
    	      System.out.println("| Rank | Word              | Frequency |");
    	      System.out.println("+------+-------------------+-----------+");
    	      
    	      for (int i=0; i < nPrint; i++)
    	    	 System.out.printf("| %4d | %17s | %9d |\n", i+1, words.get(i).word, words.get(i).count); 
       	    System.out.println("+------+-------------------+-----------+");
       	      
    	      System.out.println("Common words");
    	      System.out.println("+------+-------------------+-----------+");
    	      System.out.println("| Rank | Word              | Frequency |");
    	      System.out.println("+------+-------------------+-----------+");
    	      
    	      for (int i=nStart; i < nEnd; i++)  
    	    	  System.out.printf("| %4d | %17s | %9d |\n", i+1, words.get(i).word, words.get(i).count);
    	      System.out.println("+------+-------------------+-----------+");
    	      // 	  System.out.println(words.get(i).word+ ", " + words.get(i).count); 
    	      
    	      System.out.println("Rare words");
    	      System.out.println("+------+-------------------+-----------+");
    	      System.out.println("| Rank | Word              | Frequency |");
    	      System.out.println("+------+-------------------+-----------+");
    	      for (int i=(nWords - nPrint); i < nWords; i++)  
       	    	  System.out.printf("| %4d | %17s | %9d |\n", i+1, words.get(i).word, words.get(i).count);
    	    	 // System.out.println(words.get(i).word+ ", " + words.get(i).count); 
    	      System.out.println("+------+-------------------+-----------+");

    	      /*
    	      characters.sort(new Comparator<CharCnt>() {
    	    	  @Override
    	    	  public int compare(CharCnt arg0, CharCnt arg1) {
    	    		if ( arg0.count == arg1.count )  return 0;
    	    		else if (arg0.count < arg1.count) return 1;
    	    		else return -1;
    	    	  			}
    	      			});
    	   */
    	      nPrint = (int) Math.ceil(nCharacters * 0.05);
    	      nStart = ( nCharacters/2 - (int) Math.ceil(nCharacters * 0.025) );
    	      nEnd = ( nCharacters/2 + (int) Math.ceil(nCharacters * 0.025) );
    	      
    	      System.out.println("------------------------------------------------------------");
    	      System.out.println("Total number of distinct letters : " + nCharacters);
    	      System.out.println("Popular threahold letters : " + nPrint);
    	      System.out.println("Common threahold l letters : " + (nStart+1));
    	      System.out.println("Common threahold u letters : " + nEnd);
    	      System.out.println("Rare threahold words : " + (nCharacters - nPrint + 1));
    	      System.out.println("------------------------------------------------------------");	 	      
    	      
    	      System.out.println("Popular letters");
    	      System.out.println("+------+--------+-----------+");
    	      System.out.println("| Rank | letter | Frequency |");
    	      System.out.println("+------+--------+-----------+");
    	      for (int i=0; i < nPrint; i++)
    	    	  System.out.printf("| %4d | %6s | %9d |\n", i+1, characters.get(i).c, characters.get(i).count);
    	    	  //System.out.println(characters.get(i).c + ", " + characters.get(i).count); 
    	      System.out.println("+------+--------+-----------+");  
    	      
    	      System.out.println("Common letters");
    	      System.out.println("+------+--------+-----------+");
    	      System.out.println("| Rank | letter | Frequency |");
    	      System.out.println("+------+--------+-----------+");
    	      for (int i=nStart; i < nEnd; i++)  
    	    	  System.out.printf("| %4d | %6s | %9d |\n", i+1, characters.get(i).c, characters.get(i).count);
    	    	  //System.out.println(characters.get(i).c + ", " + characters.get(i).count); 
    	      System.out.println("+------+--------+-----------+");
    	      
    	      System.out.println("Rare letters");
    	      System.out.println("+------+--------+-----------+");
    	      System.out.println("| Rank | letter | Frequency |");
    	      System.out.println("+------+--------+-----------+");
    	      for (int i=(nCharacters - nPrint); i < nCharacters; i++) 
    	    	  System.out.printf("| %4d | %6s | %9d |\n", i+1, characters.get(i).c, characters.get(i).count);
    	    	  //System.out.println(characters.get(i).c + ", " + characters.get(i).count);     		
    	      System.out.println("+------+--------+-----------+");
    	}

    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
      String[] remainingArgs = optionParser.getRemainingArgs();

      if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
        System.err.println("Usage: letterwordcount <in> <out> [-skip skipPatternFile]");
        System.exit(2);
      		 }
      
      Job job = Job.getInstance(conf, "letter word count");
      job.setJarByClass(LetterWordCount.class);
      job.setMapperClass(TokenizerMapper.class);
      job.setCombinerClass(IntSumReducer.class);
      job.setReducerClass(IntSumReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      List<String> otherArgs = new ArrayList<String>();
      for (int i=0; i < remainingArgs.length; ++i) {
        if ("-skip".equals(remainingArgs[i])) {
          job.addCacheFile(new Path(remainingArgs[++i]).toUri());
          job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
        } else {
          otherArgs.add(remainingArgs[i]);
        		  }
      		 }
      FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
      FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

      if ( job.waitForCompletion(true) == false )
    	  		System.exit(1);	

      postProcess(conf, FileOutputFormat.getOutputPath(job));
      System.exit(0);	
    }
    
  }



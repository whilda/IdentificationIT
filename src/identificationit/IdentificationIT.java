package identificationit;

/**
 *
 * @author (13511601) Whilda Chaq
 */

import java.io.*;
import static java.lang.Thread.sleep;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.GenericOptionsParser;
import sourceafis.simple.AfisEngine;
import sourceafis.simple.Fingerprint;
import sourceafis.simple.Person;

public class IdentificationIT{
        static class ValueComparator implements Comparator<String> {
            Map<String, Float> base;
            public ValueComparator(Map<String, Float> base) {
                this.base = base;
            }

            @Override
            public int compare(String a, String b) {
                if (base.get(a) >= base.get(b)) {
                    return -1;
                } else {
                    return 1;
                }
            }
        }
                
        public static AfisEngine FIS = new AfisEngine();
        public static Fingerprint fpQuery = new Fingerprint();
        public static Person queryPerson = new Person(fpQuery);
        public static Fingerprint fpData = new Fingerprint();
        public static Person dataPerson = new Person(fpData);
        
        private static final HashMap<String,Float> map = new HashMap<>();
        private static final ValueComparator bvc = new ValueComparator(map);
        private static final TreeMap<String,Float> sorted_map = new TreeMap<>(bvc);
        
        public static void main (String [] args) throws Exception{
                try{
                    Date startDate = new Date();
                    MeasurementThread mr = new MeasurementThread("identificationIT", Runtime.getRuntime());
                    mr.start();
                    sleep(500);
                    Configuration conf = new Configuration();
                    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                    String paramK = "";
                    String paramThreshold = "";
                    String paramPathQuery = "";
                    String paramPathInput = "";
                    String paramPathOutput = "";
                    if (otherArgs.length == 4) {
                        paramK = otherArgs[0];
                        paramThreshold = otherArgs[1];
                        if(otherArgs[2].equals("1")){
                            paramPathQuery = "/Query/FVC2000-Db1_b_105_8.template";
                            paramPathInput = "/Percobaan1";
                        }else if(otherArgs[2].equals("2")){
                            paramPathQuery = "/Query/FVC2002-Db4_a_22_4.template";
                            paramPathInput = "/Percobaan2";
                        }else if(otherArgs[2].equals("3")){
                            paramPathQuery = "/Query/FVC2004-Db3_a_99_3.template";
                            paramPathInput = "/Percobaan3";
                        }else if(otherArgs[2].equals("4")){
                            paramPathQuery = "/Query/FVC2006-Db2_a_84_11.template";
                            paramPathInput = "/Percobaan4";
                        }
                        paramPathOutput = otherArgs[3];
                    }else if (otherArgs.length == 5) {
                        paramK = otherArgs[0];
                        paramThreshold = otherArgs[1];
                        paramPathQuery = otherArgs[2];
                        paramPathInput = otherArgs[3];
                        paramPathOutput = otherArgs[4];
                    }else{
                        System.err.println("Usage: IdentificationIT <K> <Th> <Q> <in> <out>");
                        System.err.println("K: number for ouput, ex : 5");
                        System.err.println("Th: threshold for matching score, ex : 10");
                        System.err.println("Q: Path to query template, ex : /dir/a.template");
                        System.err.println("in: Path to directory input, ex : /dir");
                        System.err.println("out: Path to file output, ex : /dir");
                        System.exit(2);
                    }
                    
                    int K = Integer.parseInt(paramK);
                    float Threshold = Float.parseFloat(paramThreshold);
                    String pathQuery = "hdfs://master:9000"+paramPathQuery;
                    String db = "hdfs://master:9000"+paramPathInput;
                    String output = paramPathOutput;
                    
                    float matchingScore;
                    
                    FileSystem hdfs = FileSystem.get( new URI( "hdfs://master:9000" ), conf);     
                    SetupQueryPerson(hdfs,pathQuery);
                    FileStatus[] statusList = hdfs.listStatus(new Path(db));
                    
                    long startTime = System.currentTimeMillis();
                    int counter = 0;
                    for (FileStatus status : statusList) {
                        if(!status.getPath().getName().contains("properties")){
                            byte[] template = unpackZip(hdfs,status.getPath());
                            if(template != null){
                                SetupDataPerson(template,status.getPath());
                                matchingScore = FIS.verify(queryPerson, dataPerson);
                                if(matchingScore >= Threshold){
                                    map.put(status.getPath().getName(), matchingScore);
                                }
                                System.out.println("Progress : "+ ((float) ++counter/statusList.length) * 100 +" %");
                            }else{
                                System.out.println("ERROR");
                            }
                        }
                    }
                    
                    sorted_map.putAll(map);
                    int i = 0;
                    for(Map.Entry<String,Float> entry : sorted_map.entrySet()) {
                        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output, true)))) 
                        {
                            out.println(entry.getKey()+"\t"+entry.getValue());
                        }catch (IOException e) {
                            System.out.println("Error write : "+e.getMessage());
                        }
                        if(++i == K) break;
                    }
                    Date endDate = new Date();
                    mr.Stop();
                    
                    DateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
                    System.out.println("Start\t: "+ df.format(startDate));
                    System.out.println("Finish\t: "+ df.format(endDate));

                    long diff = endDate.getTime() - startDate.getTime();
                    System.out.println("Time (ms)\t: "+ diff);
                    mr.PrintAverage();
                }catch(IOException | IllegalArgumentException e){
                    System.out.println("ERROR : "+e.getMessage());
                }
        }
        private static void SetupQueryPerson(FileSystem hdfs,String pathQuery) throws IOException {
            fpQuery.setIsoTemplate(GetByteQuery(hdfs,pathQuery));
        }
        private static void SetupDataPerson(byte[] template,Path path) throws IOException {
            try{
                fpData.setIsoTemplate(template);
            }catch(Exception ex){
                System.out.println("Error Path : "+path.getName());
            }
        }
         private static byte[] GetByteQuery(FileSystem hdfs,String pathQuery) throws IOException{
            InputStream is = hdfs.open(new Path(pathQuery));
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();

            int nRead;
            byte[] data = new byte[16384];

            while ((nRead = is.read(data, 0, data.length)) != -1) {
              buffer.write(data, 0, nRead);
            }

            buffer.flush();

            return buffer.toByteArray();
        }
        private static byte[] unpackZip(FileSystem fs,Path path)
        {       
             FSDataInputStream is;
             ZipInputStream zis;
             byte[] result = null;
             try 
             {
                is = new FSDataInputStream(fs.open(path));
                zis = new ZipInputStream(new BufferedInputStream(is));          
                ZipEntry ze;
                byte[] buffer = new byte[1024];
                int count;
                ze = zis.getNextEntry();
                if(ze != null){
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    while ((count = zis.read(buffer)) != -1) 
                    {
                        bos.write(buffer, 0, count); 
                    }
                    bos.close();               
                    zis.closeEntry();
                    result = bos.toByteArray();
                }
                zis.close();
             } 
             catch(IOException e)
             {
                 System.out.println("ERROR : "+e.getMessage());
                 return null;
             }

            return result;
        }
}

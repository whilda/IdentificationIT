package identificationit;

/**
 *
 * @author (13511601) Whilda Chaq
 */

import java.io.*;
import java.net.URI;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
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
                    int K = 20000;
                    float Threshold = -2;
                    String db = "hdfs://localhost:9000/db/zip-Summary";
                    String pathQuery = "hdfs://localhost:9000/query/FVC2000-Db1_a_1_1.template";
                    float matchingScore;
                    FileSystem hdfs = FileSystem.get( new URI( "hdfs://localhost:9000" ), new Configuration());     
                    SetupQueryPerson(hdfs,pathQuery);
                    FileStatus[] statusList = hdfs.listStatus(new Path(db));
                    
                    float startTime = System.currentTimeMillis();
                    int counter = 0;
                    for (FileStatus status : statusList) {
                        if(!status.getPath().getName().contains("properties")){
                            byte[] template = unpackZip(hdfs,status.getPath());
                            if(template != null){
                                SetupDataPerson(template,status.getPath());
                                matchingScore = FIS.verify(queryPerson, dataPerson);
                                if(matchingScore >= Threshold){
                                //if(matchingScore != 0.0){
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
                    for(Map.Entry<String,Float> entry : map.entrySet()) {
                        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("/home/whilda/info", true)))) {
                            out.println(entry.getKey()+"\t"+entry.getValue());
                        }catch (IOException e) {
                            System.out.println("Error write : "+e.getMessage());
                        }
                        if(++i == K) break;
                    }
                    float endTime = System.currentTimeMillis();
                    System.out.println("Time : "+((float)(endTime-startTime))+" s");
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
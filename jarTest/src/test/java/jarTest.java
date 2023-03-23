import static org.junit.Assert.assertEquals;
import org.junit.Test;
import io.delta.sharing.server.SharedTableManager;
import io.delta.sharing.server.config.ServerConfig;
import io.delta.sharing.server.config.TableConfig;
import io.delta.sharing.server.model.SingleAction;
import io.delta.sharing.server.protocol.*;
import io.delta.sharing.server.protocol.GetShareResponse;
import io.delta.sharing.server.protocol.Share;
import io.delta.standalone.internal.DeltaSharedTableLoader;
import scala.None;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import java.util.Arrays;

public class jarTest {

    @Test
    public void evaluateGetShare() {
        SharedTableManager tableManager = getSharedTableManager();
        Share s = tableManager.getShare("share1");
        assertEquals("share1", s.getName());
    }

    @Test
    public void evaluateListShare(){
        SharedTableManager tableManager = getSharedTableManager();
        Option<String> nextPageToken= Option.apply(null);
        Option<Object> maxResult = Option.apply(500);
        var result = tableManager.listShares(nextPageToken, maxResult);

        Seq<Share> shares = result._1();
        int i=0;
        for (Share share : JavaConverters.seqAsJavaList(shares)) {
            if(i==0){
                assertEquals("share1",share.getName());
            }
            if(i==1){
                assertEquals("share2",share.getName());
            }
            i++;

        }
    }

    @Test
    public void evaluateListTables(){
        ServerConfig config =  ServerConfig.load("src/test/conf/delta-sharing-server.yaml");
        SharedTableManager tableManager = new SharedTableManager(config);
        Option<String> nextPageToken= Option.empty();
        Option<Object> maxResult = Option.apply(500);
        var result = tableManager.listAllTables("share1",nextPageToken,maxResult);
        Seq<Table> tableSeq = result._1();
        int i=0;
        for (Table table : JavaConverters.seqAsJavaList(tableSeq)) {
            if(i==0){
                assertEquals("test1",table.getName());
            }
            else if(i==1){
                assertEquals("test2",table.getName());
            }
            i++;
        }
    }

    @Test
    public void evaluateListScehmas(){
        SharedTableManager tableManager = getSharedTableManager();
        Option<String> nextPageToken= Option.apply(null);
        Option<Object> maxResult = Option.apply(500);
        var result = tableManager.listSchemas("share1",nextPageToken,maxResult);
        Seq<Schema> schemaSeq = result._1();
        for (Schema schema : JavaConverters.seqAsJavaList(schemaSeq)) {
            assertEquals("schema1",schema.getName());
        }
    }

    @Test
    public void evaluateQuery(){
        SharedTableManager tableManager = getSharedTableManager();
        DeltaSharedTableLoader tableLoader = new DeltaSharedTableLoader(getServerConfig());
        TableConfig tableConfig = tableManager.getTable("share1","schema1","test1");
        Option<Object> noneObject = Option.empty();
        Option<String> nextPageToken= Option.apply(null);
        Option<String> timeStamp = Option.apply(null);
        Seq<String> predicateHint = JavaConverters.asScalaBuffer(Arrays.asList("data", ">=", "'2021-01-01'")).seq();
        var result = tableLoader.loadTable(tableConfig).query(true,predicateHint,noneObject,noneObject,timeStamp,noneObject);
        Seq<SingleAction> actions = result._2();
        int i=0;
        for (SingleAction action : JavaConverters.seqAsJavaList(actions)) {
            if(action.file()!=null){
                if(i==0){
                    assertEquals("{\"numRecords\":1,\"minValues\":{\"id\":1,\"data\":\"batch1\",\"category\":\"cat\"},\"maxValues\":{\"id\":1,\"data\":\"batch1\",\"category\":\"cat\"},\"nullCount\":{\"id\":0,\"data\":0,\"category\":0}}",action.file().stats());
                }
                else if(i==1){
                    assertEquals("{\"numRecords\":1,\"minValues\":{\"id\":1,\"data\":\"batch1\",\"category\":\"cat\"},\"maxValues\":{\"id\":1,\"data\":\"batch1\",\"category\":\"cat\"},\"nullCount\":{\"id\":0,\"data\":0,\"category\":0}}",action.file().stats());
                }
                else if(i==2){
                    assertEquals("{\"numRecords\":1,\"minValues\":{\"id\":3,\"data\":\"batch3\",\"category\":\"mouse\"},\"maxValues\":{\"id\":3,\"data\":\"batch3\",\"category\":\"mouse\"},\"nullCount\":{\"id\":0,\"data\":0,\"category\":0}}",action.file().stats());
                }
                else if(i==3){
                    assertEquals("{\"numRecords\":1,\"minValues\":{\"id\":3,\"data\":\"batch3\",\"category\":\"mouse\"},\"maxValues\":{\"id\":3,\"data\":\"batch3\",\"category\":\"mouse\"},\"nullCount\":{\"id\":0,\"data\":0,\"category\":0}}",action.file().stats());
                }
                else if(i==4){
                    assertEquals("{\"numRecords\":1,\"minValues\":{\"id\":2,\"data\":\"batch2\",\"category\":\"dog\"},\"maxValues\":{\"id\":2,\"data\":\"batch2\",\"category\":\"dog\"},\"nullCount\":{\"id\":0,\"data\":0,\"category\":0}}",action.file().stats());
                }
                i++;

            }

        }
    }

    public SharedTableManager getSharedTableManager(){

        return new SharedTableManager(getServerConfig());
    }

    public ServerConfig getServerConfig(){
        String jsonInput = "{\n" +
                "  \"version\": 1,\n" +
                "  \"shares\": [\n" +
                "    {\n" +
                "      \"name\": \"share1\",\n" +
                "      \"schemas\": [\n" +
                "        {\n" +
                "          \"name\": \"schema1\",\n" +
                "          \"tables\": [\n" +
                "            {\n" +
                "              \"name\": \"test1\",\n" +
                "              \"location\": \"s3a://hdl-test-data-eu-central-1/delta_lake/tab/\"\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"share2\",\n" +
                "      \"schemas\": [\n" +
                "        {\n" +
                "          \"name\": \"schema2\",\n" +
                "          \"tables\": [\n" +
                "            {\n" +
                "              \"name\": \"test2\",\n" +
                "              \"location\": \"s3a://hdl-test-data-eu-central-1/delta_lake/tab_demo1/\"\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"authorization\": {\n" +
                "    \"bearerToken\": \"123456\"\n" +
                "  },\n" +
                "  \"host\": \"localhost\",\n" +
                "  \"port\": 8081,\n" +
                "  \"endpoint\": \"/delta_sharing\",\n" +
                "  \"preSignedUrlTimeoutSeconds\": 3600,\n" +
                "  \"deltaTableCacheSize\": 10,\n" +
                "  \"stalenessAcceptable\": false,\n" +
                "  \"evaluatePredicateHints\": false\n" +
                "}";
        // ServerConfig config = ServerConfig.load("/Users/i574237/delta_sharing/delta-sharing/delta-sharing-server.yaml");
        ServerConfig config = ServerConfig.loadJson(jsonInput);
        return config;
    }

}

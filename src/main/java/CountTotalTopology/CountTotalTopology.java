package CountTotalTopology;

import CountTopology.LineReaderSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by Wesley on 12/5/15.
 */
public class CountTotalTopology {

    public static void main(String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new LineReaderSpout());
        builder.setBolt("split", new CopyBolt()).shuffleGrouping("spout");
        builder.setBolt("count", new CumulateBolt()).fieldsGrouping("split", new Fields("word"));
        Config conf = new Config();
        conf.put("inputFile", args[1]);
        conf.put("outputFile", args[2]);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 5);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }

        System.out.println("Finished");

    }
}

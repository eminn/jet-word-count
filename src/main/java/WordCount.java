import com.hazelcast.core.IMap;
import com.hazelcast.jet.AggregateOperations;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Partitioner;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.function.DistributedFunctions;
import com.hazelcast.jet.processor.Processors;
import com.hazelcast.jet.processor.Sinks;
import com.hazelcast.jet.processor.Sources;
import com.hazelcast.jet.server.JetBootstrap;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import static com.hazelcast.jet.Edge.between;

public class WordCount {
    public static void main(String[] args) throws Exception {

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", Sources.readMap("lines"));
        // (lineNum, line) -> words
        Pattern delimiter = Pattern.compile("\\W+");
        Vertex tokenize = dag.newVertex("tokenize",
                Processors.flatMap((Entry<Integer, String> e) ->
                        Traversers.traverseArray(delimiter.split(e.getValue().toLowerCase()))
                                  .filter(word -> !word.isEmpty()))
        );

        // word -> (word, count)
        Vertex accumulate = dag.newVertex("accumulate",
                Processors.accumulateByKey(
                        DistributedFunctions.wholeItem(),
                        AggregateOperations.counting())
        );

        // (word, count) -> (word, count)
        Vertex combine = dag.newVertex("combine",
                Processors.combineByKey(AggregateOperations.counting())
        );

        Vertex sink = dag.newVertex("sink", Sinks.writeMap("counts"));

        dag.edge(between(source, tokenize))
           .edge(between(tokenize, accumulate)
                   .partitioned(DistributedFunctions.wholeItem(), Partitioner.HASH_CODE))
           .edge(between(accumulate, combine)
                   .distributed()
                   .partitioned(DistributedFunctions.entryKey()))
           .edge(between(combine, sink));


        JetInstance jet = JetBootstrap.getInstance();
        IMap<Integer, String> map = jet.getMap("lines");
        map.put(0, "It was the best of times,");
        map.put(1, "it was the worst of times,");
        map.put(2, "it was the age of wisdom,");
        map.put(3, "it was the age of foolishness,");
        map.put(4, "it was the epoch of belief,");
        map.put(5, "it was the epoch of incredulity,");
        map.put(6, "it was the season of Light,");
        map.put(7, "it was the season of Darkness");
        map.put(8, "it was the spring of hope,");
        map.put(9, "it was the winter of despair,");
        map.put(10, "we had everything before us,");
        map.put(11, "we had nothing before us,");
        map.put(12, "we were all going direct to Heaven,");
        map.put(13, "we were all going direct the other way --");
        map.put(14, "in short, the period was so far like the present period, that some of "
                + "its noisiest authorities insisted on its being received, for good or for "
                + "evil, in the superlative degree of comparison only.");

        Job job = jet.newJob(dag);
        Future<Void> future = job.execute();
        future.get();
        System.out.println(jet.getMap("counts").entrySet());

    }
}

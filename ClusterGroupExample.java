import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
public class ClusterGroupExample {
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            if (!ExamplesUtils.checkMinTopologySize(ignite.cluster(), 2))
                return;

            System.out.println();
            System.out.println("Compute example started.");

            IgniteCluster cluster = ignite.cluster();

            // Say hello to all nodes in the cluster, including local node.
            sayHello(ignite, cluster);

            // Say hello to all remote nodes.
            sayHello(ignite, cluster.forRemotes());

            // Pick random node out of remote nodes.
            ClusterGroup randomNode = cluster.forRemotes().forRandom();

            // Say hello to a random node.
            sayHello(ignite, randomNode);

            // Say hello to all nodes residing on the same host with random node.
            sayHello(ignite, cluster.forHost(randomNode.node()));

            // Say hello to all nodes that have current CPU load less than 50%.
            sayHello(ignite, cluster.forPredicate(n -> n.metrics().getCurrentCpuLoad() < 0.5));
        }
    }
    private static void sayHello(Ignite ignite, final ClusterGroup grp) throws IgniteException {
        // Print out hello message on all cluster nodes.
        ignite.compute(grp).broadcast(
            () -> System.out.println(">>> Hello Node: " + grp.ignite().cluster().localNode().id()));
    }
}
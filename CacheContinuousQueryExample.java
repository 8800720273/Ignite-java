import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryUpdatedListener;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;

public class CacheContinuousQueryExample {
	 private static final String CACHE_NAME = CacheContinuousQueryExample.class.getSimpleName();

	    /**
	     * Executes example.
	     *
	     * @param args Command line arguments, none required.
	     * @throws Exception If example execution failed.
	     */
	    public static void main(String[] args) throws Exception {
	        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
	            System.out.println();
	            System.out.println(">>> Cache continuous query example started.");

	            // Auto-close cache at the end of the example.
	            try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME)) {
	                int keyCnt = 20;

	                // These entries will be queried by initial predicate.
	                for (int i = 0; i < keyCnt; i++)
	                    cache.put(i, Integer.toString(i));

	                // Create new continuous query.
	                ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();

	                qry.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<Integer, String>() {
	                    @Override public boolean apply(Integer key, String val) {
	                        return key > 10;
	                    }
	                }));

	                // Callback that is called locally when update notifications are received.
	                qry.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {
	                    @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> evts) {
	                        for (CacheEntryEvent<? extends Integer, ? extends String> e : evts)
	                            System.out.println("Updated entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');
	                    }
	                });

	                // This filter will be evaluated remotely on all nodes.
	                // Entry that pass this filter will be sent to the caller.
	                qry.setRemoteFilterFactory(new Factory<CacheEntryEventFilter<Integer, String>>() {
	                    @Override public CacheEntryEventFilter<Integer, String> create() {
	                        return new CacheEntryEventFilter<Integer, String>() {
	                            @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends String> e) {
	                                return e.getKey() > 10;
	                            }
	                        };
	                    }
	                });

	                // Execute query.
	                try (QueryCursor<Cache.Entry<Integer, String>> cur = cache.query(qry)) {
	                    // Iterate through existing data.
	                    for (Cache.Entry<Integer, String> e : cur)
	                        System.out.println("Queried existing entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');

	                    // Add a few more keys and watch more query notifications.
	                    for (int i = keyCnt; i < keyCnt + 10; i++)
	                        cache.put(i, Integer.toString(i));

	                    // Wait for a while while callback is notified about remaining puts.
	                    Thread.sleep(2000);
	                }
	            }
	            finally {
	                // Distributed cache could be removed from cluster only by #destroyCache() call.
	                ignite.destroyCache(CACHE_NAME);
	            }
	        }
	    }
	}

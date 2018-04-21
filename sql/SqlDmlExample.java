import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.model.Organization;
import org.apache.ignite.examples.model.Person;

public class SqlDmlExample {
    private static final String ORG_CACHE = SqlDmlExample.class.getSimpleName() + "Organizations";

    private static final String PERSON_CACHE = SqlDmlExample.class.getSimpleName() + "Persons";
    @SuppressWarnings({"unused", "ThrowFromFinallyBlock"})
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            print("Cache query DML example started.");

            CacheConfiguration<Long, Organization> orgCacheCfg = new CacheConfiguration<>(ORG_CACHE);
            orgCacheCfg.setIndexedTypes(Long.class, Organization.class);

            CacheConfiguration<Long, Person> personCacheCfg = new CacheConfiguration<>(PERSON_CACHE);
            personCacheCfg.setIndexedTypes(Long.class, Person.class);

            // Auto-close cache at the end of the example.
            try (
                IgniteCache<Long, Organization> orgCache = ignite.getOrCreateCache(orgCacheCfg);
                IgniteCache<Long, Person> personCache = ignite.getOrCreateCache(personCacheCfg)
            ) {
                insert(orgCache, personCache);
                select(personCache, "Insert data");

                update(personCache);
                select(personCache, "Update salary for Master degrees");

                delete(personCache);
                select(personCache, "Delete non-Apache employees");
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(PERSON_CACHE);
                ignite.destroyCache(ORG_CACHE);
            }

            print("Cache query DML example finished.");
        }
    }
    private static void insert(IgniteCache<Long, Organization> orgCache, IgniteCache<Long, Person> personCache) {
        // Insert organizations.
        SqlFieldsQuery qry = new SqlFieldsQuery("insert into Organization (_key, id, name) values (?, ?, ?)");

        orgCache.query(qry.setArgs(1L, 1L, "ASF"));
        orgCache.query(qry.setArgs(2L, 2L, "Eclipse"));

        // Insert persons.
        qry = new SqlFieldsQuery(
            "insert into Person (_key, id, orgId, firstName, lastName, salary, resume) values (?, ?, ?, ?, ?, ?, ?)");

        personCache.query(qry.setArgs(1L, 1L, 1L, "John", "Doe", 4000, "Master"));
        personCache.query(qry.setArgs(2L, 2L, 1L, "Jane", "Roe", 2000, "Bachelor"));
        personCache.query(qry.setArgs(3L, 3L, 2L, "Mary", "Major", 5000, "Master"));
        personCache.query(qry.setArgs(4L, 4L, 2L, "Richard", "Miles", 3000, "Bachelor"));
    }
    private static void update(IgniteCache<Long, Person> personCache) {
        String sql =
            "update Person set salary = salary * 1.1 " +
            "where resume = ?";

        personCache.query(new SqlFieldsQuery(sql).setArgs("Master"));
    }
    private static void delete(IgniteCache<Long, Person> personCache) {
        String sql = "delete from Person where orgId != ?";

        personCache.query(new SqlFieldsQuery(sql).setArgs(1)).getAll();
    }
    private static void select(IgniteCache<Long, Person> personCache, String msg) {
        String sql =
            "select p.id, concat(p.firstName, ' ', p.lastName), o.name, p.resume, p.salary " +
            "from Person as p, \"" + ORG_CACHE + "\".Organization as o " +
            "where p.orgId = o.id";

        List<List<?>> res = personCache.query(new SqlFieldsQuery(sql).setDistributedJoins(true)).getAll();

        print(msg);

        for (Object next : res)
            System.out.println(">>>     " + next);
    }
    private static void print(String msg) {
        System.out.println();
        System.out.println(">>> " + msg);
    }
}

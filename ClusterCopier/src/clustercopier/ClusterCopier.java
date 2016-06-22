package clustercopier;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Tuple;

public class ClusterCopier {
	private static Logger log = Logger.getLogger(ClusterCopier.class);
	private static volatile boolean shutdown = false;

	private static final String shosts = System.getProperty("shosts", null); // source hosts
	private static final String dhost = System.getProperty("dhost", null); // destination host (one master in the cluster)
	private static final String match = System.getProperty("match", "*"); // the key pattern to match

	// To avoid a round trip to get the type of the key, it can be specified on the command line.
	// Records not of this type are discarded.
	private static final String dataType = System.getProperty("type", null);

	// for debugging we may want to limit the number of transfers by limiting
	// how long we copy.  This is the number of seconds to run
	private static final int runtime = Integer.valueOf(System.getProperty("time", "" + Integer.MAX_VALUE));

	private static AtomicLong totalProcessed = new AtomicLong(0);
	private static AtomicLong processed = new AtomicLong(0);
	private static AtomicLong totalFailed = new AtomicLong(0);

	static {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				shutdown = true;
			}
		}));
	}

	static private void usage() {
		System.err.println("Usage: java -Dshosts=host1:port1,... -Ddhost=host2:port2 -Dtype=... -Dtime=<seconds> -Dmatch='...' -cp <jarname> clustercopier.ClusterCopier \n" + //
				"Where:\n" + //
				"\tshosts is the source cluster or a list of non-cluster redis instances.\n" + //
				"\t\tIf the first element of the list is a cluster element then the copy will treat the list as references to a single cluster.\n" + //
				"\t\tIf the first element of the list is a non-cluster instance then the list will be treated as a series of instances to copy from.\n" + //
				"\t\tOnly one (master) instance from a cluster is needed.\n" + //
				"\tdhost is the destination cluster or redis instance.  Only one instance may be specified.\n" + //
				"\ttype is the optional redis data type of the data to be copy.  Records not of this type are skipped.\n" + //
				"\ttime is the optional length of time to run in seconds.  Usefull for development purpose.  Default is as long as it takes." + //
				"\tmatch is the redis pattern to use in the scan to copy.  Defaults to all keys");
	}

	private static GenericObjectPoolConfig initPoolConfiguration() {
		GenericObjectPoolConfig config = new GenericObjectPoolConfig();

		config.setLifo(true);
		config.setTestOnBorrow(true);
		config.setTestOnReturn(false);
		config.setBlockWhenExhausted(true);
		// config.setMinIdle(Math.max(1, (int) (poolSize / 10))); // keep 10% hot always
		config.setMinIdle(1);
		config.setMaxTotal(20); // the number of Jedis connections to create
		config.setTestWhileIdle(false);
		config.setSoftMinEvictableIdleTimeMillis(3000L);
		config.setNumTestsPerEvictionRun(5);
		config.setTimeBetweenEvictionRunsMillis(5000L);
		config.setJmxEnabled(true);

		return config;
	}

	private static JedisCluster initCluster(String hosts) {
		JedisCluster jedisCluster = null;
		if (hosts == null || hosts.trim().length() == 0) {
			log.fatal("NULL hosts list");
			usage();
			System.exit(1);
			return jedisCluster;
		}

		try {
			String[] jcHosts = hosts.split("\\s*,\\s*");
			Set<HostAndPort> jedisClusterNodes = new HashSet<>();

			for (String jcHost : jcHosts) {
				String[] parts = jcHost.split("\\s*:\\s*");
				try {
					InetAddress address = InetAddress.getByName(parts[0]);
					jedisClusterNodes.add(new HostAndPort(address.getHostAddress(), Integer.valueOf(parts[1])));
				}
				catch (Exception e) {
					log.fatal("Problem parsing jedis cluster host name: " + jcHost + "; " + e);
					usage();
					System.exit(1);
				}
			}

			if (jedisClusterNodes.size() == 0) {
				log.fatal("No usable jedis cluster host addresses found from " + hosts);
				usage();
				System.exit(1);
			}

			GenericObjectPoolConfig config = initPoolConfiguration();
			jedisCluster = new JedisCluster(jedisClusterNodes, config);

		}
		catch (Exception e) {
			log.fatal("Failed to create JedisCluster: " + e, e);
			usage();
			System.exit(1);
		}

		return jedisCluster;
	}

	final static Timer progressTimer = new Timer(ClusterCopier.class.getSimpleName() + "-progressTimer", true);
	final static TimerTask progressNotifier = new TimerTask() {
		@Override
		public void run() {
			log.info("total: " + totalProcessed.longValue() + ", delta: " + processed.getAndSet(0));
			if (shutdown)
				progressTimer.cancel();
		}
	};

	/**
	 * BoundedExecutor
	 * <p/>
	 * Using a Semaphore to throttle task submission
	 *
	 * @author Brian Goetz and Tim Peierls
	 */
	static class BoundedExecutor {
		private final Executor exec;
		private final Semaphore semaphore;

		public BoundedExecutor(Executor exec, int bound) {
			this.exec = exec;
			this.semaphore = new Semaphore(bound);
		}

		public void submitTask(final Runnable command) throws InterruptedException {
			semaphore.acquire();
			try {
				exec.execute(new Runnable() {
					public void run() {
						try {
							command.run();
						}
						finally {
							semaphore.release();
						}
					}
				});
			}
			catch (RejectedExecutionException e) {
				semaphore.release();
			}
		}

		public Semaphore getSemaphore() {
			return semaphore;
		}
	}

	private static boolean isCluster(String[] sourceList) {
		boolean isCluster = false;

		if (sourceList != null) {
			try {
				// only examine the first element in the list
				String[] parts = sourceList[0].split("\\s*:\\s*");
				Integer port = Integer.valueOf(parts[1]);

				try (Jedis j = new Jedis(parts[0], port.intValue())) {
					String info = j.info().toString();
					if (info.contains("cluster_enabled")) {
						String infoItems[] = info.split("\n");
						for (String item : infoItems) {
							if (!item.trim().startsWith("cluster_enabled"))
								continue;
							String[] ce = item.split("\\s*:\\s*");
							if (ce[1].trim().startsWith("1"))
								isCluster = true;
							break;
						}
					}
				}
			}
			catch (Exception e) {
				log.error(e);
				System.exit(1);
			}
		}

		return isCluster;
	}

	public static void main(String[] args) {
		Logger rootLogger = Logger.getRootLogger();
		if (!rootLogger.getAllAppenders().hasMoreElements()) {
			rootLogger.setLevel(Level.INFO);
			rootLogger.addAppender(new ConsoleAppender(new org.apache.log4j.PatternLayout("%d{yyyy-MM-dd HH:mm:ss.SSS} [%t; %C{1}] %-5p -- %m%n")));
		}

		log.info("Starting copy from " + shosts + " to " + dhost);
		long start = System.currentTimeMillis();

		String[] sourceList = shosts.split("\\s*,\\s*");
		if (sourceList == null || sourceList.length == 0) {
			log.error("Source list is empty");
			System.exit(1);
		}

		final boolean processSourceAsCluster = isCluster(sourceList);

		String[] destinationList = dhost.split("\\s*,\\s*");
		if (destinationList == null || destinationList.length == 0) {
			log.error("destination list is empty");
			System.exit(1);
		}

		final boolean processDestinationAsCluster = isCluster(destinationList);

		progressTimer.scheduleAtFixedRate(progressNotifier, 10 * 1000, 10 * 1000);

		ExecutorService executor = Executors.newFixedThreadPool(100 + 10); // add 10 so there's never a rejection
		BoundedExecutor boundedExecutor = new BoundedExecutor(executor, 100); // limit to 100 threads (110 > 100)

		class ScanProcessor implements ScannerAction {
			private JedisCommands source;
			private JedisCommands destination;

			public ScanProcessor(JedisCommands source, JedisCommands destination) {
				this.source = source;
				this.destination = destination;
			}

			@Override
			public void action(String key) {
				final String k = key;
				try {
					boundedExecutor.submitTask(new Runnable() {
						@Override
						public void run() {
							processKey(k);
						}
					});
				}
				catch (InterruptedException e) {
					log.error("Error processing key " + key + "; " + e);
				}
			}

			// scan key processing

			private void processKey(String key) {
				totalProcessed.incrementAndGet();
				processed.incrementAndGet();
				String type = null;
				Long ttl = 0L;

				try {
					if (dataType != null)
						type = dataType;
					else
						type = source.type(key);

					ttl = source.ttl(key);
					boolean keyExists = true;

					switch (type.toLowerCase()) {
						case "hash": {
							Map<String, String> values = source.hgetAll(key);
							if (values != null && values.size() != 0)
								destination.hmset(key, values);
							else
								keyExists = false;

							break;
						}

						case "string": {
							String value = source.get(key);
							if (value != null && value.length() != 0)
								destination.set(key, value);
							else
								keyExists = false;
							break;
						}

						case "list": {
							List<String> value = source.lrange(key, 0L, -1L);
							if (value != null && value.size() != 0) {
								destination.del(key);
								destination.lpush(key, value.toArray(new String[0]));
							}
							else
								keyExists = false;
							break;
						}

						case "set": {
							Set<String> value = source.smembers(key);
							if (value != null && value.size() != 0)
								destination.sadd(key, value.toArray(new String[0]));
							else
								keyExists = false;
							break;
						}

						case "zset": {
							Set<Tuple> value = source.zrangeWithScores(key, 0L, -1L);
							if (value != null && value.size() != 0) {
								HashMap<String, Double> scoreMembers = new HashMap<>();
								for (Tuple tuple : value) {
									scoreMembers.put(tuple.getElement(), tuple.getScore());
								}
								//destination.del(key);
								destination.zadd(key, scoreMembers);
							}
							else
								keyExists = false;
							break;
						}

						case "none": {
							// does not currently exist
							keyExists = false;
							break;
						}

						default:
							totalFailed.incrementAndGet();
							log.error("Failed to process key '" + key + "' of type '" + type + "'");
							keyExists = false;
							break;
					}

					if (keyExists) {
						switch (ttl.intValue()) {
							case -2: // Well, this is weird, the key exists but the ttl says it does not
								break;
							case -1:
								// no ttl exists
								break;
							default:
								destination.expire(key, ttl.intValue());
						}
					}

				}
				catch (Exception e) {
					totalFailed.incrementAndGet();
					log.error("Failed to copy key. Skipping '" + key + "': " + e);
				}
			}
		}

		/* *** */

		JedisCluster sourceCluster = processSourceAsCluster ? initCluster(shosts) : null;
		JedisCluster destinationCluster = processDestinationAsCluster ? initCluster(dhost) : null;

		ScanProcessor action = null;

		try {
			if (sourceCluster != null) {
				// source is a cluster
				if (destinationCluster != null) {
					// cluster -> cluster
					action = new ScanProcessor(sourceCluster, destinationCluster);
					ClusterScanner scanner = new ClusterScanner(sourceCluster, action);
					scanner.scan(match, runtime);
				}
				else {
					// cluster -> instance
					String[] parts = destinationList[0].split("\\s*:\\s*");
					String host = parts[0];
					Integer port = Integer.valueOf(parts[1]);

					JedisPool destination = new JedisPool(initPoolConfiguration(), host, port.intValue());
					action = new ScanProcessor(sourceCluster, new JedisOps(destination));
					ClusterScanner scanner = new ClusterScanner(sourceCluster, action);
					scanner.scan(match, runtime);
				}
			}
			else {
				// source is one or more non-clustered redis instances
				ArrayList<JedisPool> sourcesInstances = new ArrayList<>();
				for (String source : sourceList) {
					String[] parts = source.split("\\s*:\\s*");
					String host = parts[0];
					Integer port = Integer.valueOf(parts[1]);
					sourcesInstances.add(new JedisPool(initPoolConfiguration(), host, port.intValue()));
				}

				if (processDestinationAsCluster) {
					for (JedisPool source : sourcesInstances) {
						action = new ScanProcessor(new JedisOps(source), destinationCluster);
						RedisScanner scanner = new RedisScanner(source.getResource(), action);
						scanner.scan(match, runtime);
					}
				}
				else {
					String[] parts = destinationList[0].split("\\s*:\\s*");
					String host = parts[0];
					Integer port = Integer.valueOf(parts[1]);

					JedisPool source = new JedisPool(initPoolConfiguration(), host, port.intValue());
					action = new ScanProcessor(new JedisOps(source), destinationCluster);
					RedisScanner scanner = new RedisScanner(source.getResource(), action);
					scanner.scan(match, runtime);
				}
			}
		}
		catch (Exception e) {
			log.error("Problem scanning: " + e, e);
		}
		finally {}

		executor.shutdown();
		try {
			executor.awaitTermination(10, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			log.error(e.getMessage());
		}
		finally {
			log.info("total processed=" + totalProcessed.longValue() + //
					" in " + ((System.currentTimeMillis() - start) / 1000.0) + " seconds");
			log.info("Total failed keys=" + totalFailed.get());
		}

		System.exit(0);
	}

}

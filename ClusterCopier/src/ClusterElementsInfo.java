import java.net.InetAddress;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import clustercopier.ClusterScanner;
import clustercopier.ScannerAction;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Tuple;

public class ClusterElementsInfo {
	private static Logger log = Logger.getLogger(ClusterElementsInfo.class);
	private static volatile boolean shutdown = false;

	private static JedisCluster source = null;

	private static final String shosts = System.getProperty("shosts", null); // source host (one master in the cluster)

	// To avoid a round trip to get the type of the key, it can be specified on the command line.
	// Records not of this type are discarded.
	private static final String dataType = System.getProperty("type", null);

	// for debugging we may want to limit the number of transfers by limiting
	// how long we copy.  This is the number of seconds to run
	private static final int runtime = Integer.valueOf(System.getProperty("time", "" + Integer.MAX_VALUE));

	private static AtomicLong totalProcessed = new AtomicLong(0);
	private static AtomicLong processed = new AtomicLong(0);

	private static ConcurrentHashMap<String, Integer> dataKeyMax = new ConcurrentHashMap<>();
	private static ConcurrentHashMap<String, Integer> dataElementMax = new ConcurrentHashMap<>();
	private static AtomicInteger longElementCount = new AtomicInteger(0);

	static {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				shutdown = true;
			}
		}));
	}

	static private void usage() {
		System.err.println("Usage: java -Dshosts=host1:port1,... -Dtype=... -DTime=<seconds> -cp <jarname> com.findology.util.jediscluster.ClusterElementsInfo \n" + //
				"Where:\n" + //
				"\tshosts is the source cluster.  It must be a cluster.  Only one instance from the cluster is needed.\n" + //
				"\ttype is the optional redis data type of the data to be copy.  Records not of this type are skipped.\n" + //
				"\ttime is the optional length of time to run in seconds.  Usefull for development purpose.  Default is as long as it takes.");
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

	final static Timer progressTimer = new Timer(ClusterElementsInfo.class.getSimpleName() + "-progressTimer", true);
	final static TimerTask progressNotifier = new TimerTask() {
		final DecimalFormat fmt = new DecimalFormat("#,###");

		@Override
		public void run() {
			log.info("total: " + fmt.format(totalProcessed.longValue()) + "; delta: " + fmt.format(processed.getAndSet(0)));
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

	public static void main(String[] args) {
		Logger rootLogger = Logger.getRootLogger();
		if (!rootLogger.getAllAppenders().hasMoreElements()) {
			rootLogger.setLevel(Level.INFO);
			rootLogger.addAppender(new ConsoleAppender(new org.apache.log4j.PatternLayout("%d{yyyy-MM-dd HH:mm:ss.SSS} [%t; %C{1}] %-5p -- %m%n")));
		}

		log.info("Starting scan of " + shosts);
		long start = System.currentTimeMillis();

		if ((source = initCluster(shosts)) == null) {
			log.fatal("Could not create cluster for " + shosts);
			usage();
			System.exit(1);
		}

		progressTimer.scheduleAtFixedRate(progressNotifier, 10 * 1000, 10 * 1000);

		ExecutorService executor = Executors.newFixedThreadPool(100 + 10); // add 10 so there's never a rejection
		BoundedExecutor boundedExecutor = new BoundedExecutor(executor, 100); // limit to 100 threads (110 > 100)

		ScannerAction action = new ScannerAction() {
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
					// TODO Auto-generated catch block
					log.error(e);
				}
			}

			private void processDataType(String type, String key) {
				Integer dt = dataKeyMax.get(type);
				if (dt == null)
					dataKeyMax.put(type, dt = 0);
				dataKeyMax.put(type, Math.max(dataKeyMax.get(type), key.length()));
			}

			private void processDataTypeElement(String type, String key, String element) {
				Integer elm = dataElementMax.get(type);
				if (elm == null)
					dataElementMax.put(type, elm = 0);
				if (element.length() > 64)
					longElementCount.incrementAndGet();
				if (dataElementMax.get(type) < element.length())
					log.info("key " + key + " has max length element");
				dataElementMax.put(type, Math.max(dataElementMax.get(type), element.length()));
			}

			private void processKey(String key) {
				totalProcessed.incrementAndGet();
				processed.incrementAndGet();

				try {
					String type = null;
					if (dataType != null)
						type = dataType;
					else
						type = source.type(key);

					switch (type.toLowerCase()) {
						case "hash": {
							Map<String, String> values = source.hgetAll(key);
							if (values != null && values.size() != 0) {
								processDataType(type, key);
								for (String v : values.values())
									processDataTypeElement(type, key, v);
							}
							break;
						}

						case "string": {
							String value = source.get(key);
							if (value != null && value.length() != 0) {
								processDataType(type, key);
								processDataTypeElement(type, key, value);
							}
							break;
						}

						case "list": {
							List<String> value = source.lrange(key, 0L, -1L);
							if (value != null && value.size() != 0) {
								processDataType(type, key);
								for (String v : value)
									processDataTypeElement(type, key, v);
							}

							break;
						}

						case "set": {
							Set<String> value = source.smembers(key);
							if (value != null && value.size() != 0) {
								processDataType(type, key);
								for (String v : value)
									processDataTypeElement(type, key, v);
							}
							break;
						}

						case "zset": {
							Set<Tuple> value = source.zrangeWithScores(key, 0L, -1L);
							if (value != null && value.size() != 0) {
								processDataType(type, key);
								for (Tuple tuple : value) {
									processDataTypeElement(type, key, tuple.getElement());
								}
							}

							break;
						}

						case "none": {
							// does not currently exist

							break;
						}

						default:
							log.error("Failed to process key '" + key + "' of type '" + type + "'");

							break;
					}
				}
				catch (Exception e) {
					log.error("Failed to process key '" + key + "': " + e);
				}
			}
		};

		try {
			ClusterScanner scanner = new ClusterScanner(source, action);
			scanner.scan("*", runtime);
		}
		catch (Exception e) {
			log.error("Problem scanning: " + e, e);
		}
		finally {
			log.info("total processed=" + totalProcessed.longValue() + //
					" in " + ((System.currentTimeMillis() - start) / 1000.0) + " seconds");
		}

		//log.info("Executor shutdown.  Awaiting termination");
		executor.shutdown();
		try {
			executor.awaitTermination(10, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			log.error(e.getMessage());
		}
		finally {
			//log.info("Executor finished");
		}

		log.info("type key length max: " + dataKeyMax);
		log.info("type element max: " + dataElementMax);
		log.info("type elements > default max (64): " + longElementCount);

		System.exit(0);
	}

}

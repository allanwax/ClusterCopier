package clustercopier;
import java.net.InetAddress;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

/**
 * Created by allan.wax on 7/27/2015.
 */
public class ClusterScanner {
	private static final Logger log = Logger.getLogger(ClusterScanner.class);
	protected static final HashMap<String, String> hostMap = new HashMap<>();
	private JedisCluster jedisCluster = null;
	private ScannerAction action;

	/* DEBUG */ protected final ConcurrentHashMap<String, AtomicLong> scanCounts = new ConcurrentHashMap<>();

	public ClusterScanner(JedisCluster jedisCluster, ScannerAction action) {
		this.jedisCluster = jedisCluster;
		this.action = action;
	}

	private String normalizeHostAndPort(String hostAndPort) {
		try {
			String hap = hostMap.get(hostAndPort);

			if (hap == null) {
				int colon = hostAndPort.lastIndexOf(':');
				InetAddress address = InetAddress.getByName(hostAndPort.substring(0, colon));
				hap = address.getHostAddress() + hostAndPort.substring(colon);
				hostMap.put(hostAndPort, hap);
			}

			return hap;
		}
		catch (Exception e) {
			return hostAndPort;
		}
	}

	private class ScannerImpl implements Callable<Long> {
		private String clusterInstance;
		private String match;
		private ScannerAction action = null;

		public ScannerImpl(String clusterInstance, String match, ScannerAction action) {
			this.clusterInstance = clusterInstance;
			this.match = match;
			this.action = action;
		}

		/**
		 * Computes a result, or throws an exception if unable to do so.
		 *
		 * @return computed result
		 * @throws Exception
		 *             if unable to compute a result
		 */
		@Override
		public Long call() throws Exception {
			long count = 0;
			int scanCount = 0;

			try {
				String hostAndPort = normalizeHostAndPort(clusterInstance);
				String[] parts = hostAndPort.split(":");
				String host = parts[0];
				int port = Integer.valueOf(parts[1]);

				try (Jedis jedis = new Jedis(host, port)) {
					ScanParams params = new ScanParams().match(match).count(100);
					String scanMarker = "0";
					ScanResult<String> results = null;

					do {
						results = jedis.scan(scanMarker, params);
						scanCount++;
						List<String> keys = results.getResult();
						if (keys != null && keys.size() > 0) {
							count += keys.size();
							for (String key : keys) {
								/* DEBUG */ scanCounts.get(hostAndPort).incrementAndGet();
								action.action(key);
							}
						}
						scanMarker = results.getStringCursor();
					} while (!scanMarker.equals("0"));
				}

				if (log.isDebugEnabled()) {
					log.debug("Found " + count + " keys for " + hostAndPort + " in " + scanCount + " scans"); /* TEST_MODE */
				}
			}
			catch (Exception e) {
				log.error("" + e);
			}

			return count;
		}

	}

	public void scan(String match) {
		scan(match, 15);
	}

	public void scan(String match, int maxSeconds) {
		if (log.isDebugEnabled()) {
			log.debug("Start scan for '" + match + "'");
		}

		Map<String, JedisPool> jedisPools = jedisCluster.getClusterNodes();

		String nodeList = null;
		Exception screwed = null;

		// get the list of nodes (masters and slaves)

		for (JedisPool pool : jedisPools.values()) {
			try {
				try (Jedis j = pool.getResource()) {
					nodeList = j.clusterNodes();
					break;
				}
				catch (Exception e1) {
					screwed = e1;
					continue;
				}
			}
			catch (Exception e) {
				// DO SOMETHING
			}
		}

		if (nodeList == null) {
			log.error("The cluster is screwed.  Can't use any members.", screwed);
			return;
		}

		String[] nodes = nodeList.split("\n");
		ArrayList<ScannerImpl> scanners = new ArrayList<>();

		// pick out the masters
		for (String node : nodes) {
			String[] info = node.split("\\s+");

			if (info[2].indexOf("fail") >= 0) {
				continue;
			}

			if (info[2].indexOf("handshake") >= 0) {
				continue;
			}

			if (info[2].indexOf("noaddr") >= 0) {
				continue;
			}

			if (info[2].indexOf("master") >= 0) {
				if (log.isDebugEnabled())
					log.debug("Setting up scanner for " + info[1]);

				/* DEBUG */ {
					AtomicLong count = scanCounts.get(info[1]);
					if (count == null)
						scanCounts.put(info[1], new AtomicLong(0));
				}

				scanners.add(new ScannerImpl(info[1], match, action));
			}
		}

		if (!scanners.isEmpty()) {
			ExecutorService executor = Executors.newCachedThreadPool();
			Thread shutdownHook = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						executor.shutdownNow();
					}
					catch (Exception e) {
						// ignore
						if (log.isDebugEnabled())
							log.error(e);
					}
				}
			});
			try {
				Runtime.getRuntime().addShutdownHook(shutdownHook);
				executor.invokeAll(scanners, maxSeconds, TimeUnit.SECONDS);
				if (log.isDebugEnabled())
					log.debug("Scans per host: " + scanCounts.toString());
			}
			catch (Exception e) {
				log.error(e);
			}
			finally {
				try {
					Runtime.getRuntime().removeShutdownHook(shutdownHook);
				}
				catch (Exception e) {
					// ignore
					if (log.isDebugEnabled())
						log.error(e);
				}
			}
		}
	}

	/* TEST */
	public static void main(String[] args) {
		Logger rootLogger = Logger.getRootLogger();
		if (!rootLogger.getAllAppenders().hasMoreElements()) {
			rootLogger.setLevel(Level.DEBUG);
			rootLogger.addAppender(new ConsoleAppender(new org.apache.log4j.PatternLayout("%d{yyyy-MM-dd HH:mm:ss.SSS} [%t; %C{1}] %-5p -- %m%n")));
		}

		Set<HostAndPort> jcNodes = new HashSet<>();
		jcNodes.add(new HostAndPort("test2", 17001));
		final JedisCluster jc = new JedisCluster(jcNodes);

		final AtomicInteger ai = new AtomicInteger(0);
		ScannerAction action = new ScannerAction() {

			@Override
			public void action(String key) {
				ai.incrementAndGet();
			}
		};
		ClusterScanner scanner = new ClusterScanner(jc, action);

		String scanFor = "*";
		long start = System.currentTimeMillis();
		scanner.scan(scanFor, 100);

		DecimalFormat numFormat = new DecimalFormat("#,###");
		log.info("Scan for '" + scanFor + "' found " + numFormat.format(ai) + " keys in " + ((System.currentTimeMillis() - start) / 1000.0) + " seconds");

		System.exit(0);
	}
}

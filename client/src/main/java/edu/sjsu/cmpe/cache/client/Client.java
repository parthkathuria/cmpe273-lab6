package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class Client {

	private final static SortedMap<Integer, String> circle = new TreeMap<Integer, String>();
	private static HashFunction hash = Hashing.md5();
	private static ArrayList<String> servers = new ArrayList<String>();
	static char[] ch = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
			'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y',
			'z' };

	public static void main(String[] args) throws Exception {
		System.out.println("Starting Cache Client...");
		servers.add("http://localhost:3000");
		servers.add("http://localhost:3001");
		servers.add("http://localhost:3002");
		for (int i = 0; i < servers.size(); i++) {
			
			add(servers.get(i), i);
		}

		for (int j = 0; j < 10; j++) {
			int bucket = Hashing.consistentHash(Hashing.md5().hashInt(j),
					circle.size());
			String server = get(bucket);
			System.out.println("Routed to Server: " + server);
			CacheServiceInterface cache = new DistributedCacheService(server);
			cache.put(j + 1, String.valueOf(ch[j]));
			System.out.println("put(" + (j + 1) + ") => " + String.valueOf(ch[j]));
			String value = cache.get(j + 1);
			System.out.println("get(" + (j + 1) + ") => " + value);

		}

		System.out.println("Exiting Cache Client...");
	}

	public static void add(String server, int i) {
		HashCode hc = hash.hashLong(i);
		circle.put(hc.asInt(), server);
	}

	public static void remove(int key) {
		int h = hash.hashLong(key).asInt();
		circle.remove(h);
	}

	public static String get(Object key) {
		if (circle.isEmpty()) {
			return null;
		}
		int h = hash.hashLong((Integer) key).asInt();
		if (!circle.containsKey(h)) {
			SortedMap<Integer, String> tailMap = circle.tailMap(h);
			h = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
		}
		return circle.get(h);
	}
}
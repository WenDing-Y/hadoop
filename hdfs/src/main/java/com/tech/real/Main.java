package com.tech.real;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class Main {	

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new ReadSpout(), 4);
		builder.setBolt("area-bolt", new GetAreaBolt()).shuffleGrouping("spout");
		builder.setBolt("longitude-bolt", new GetLongitudeBolt()).shuffleGrouping("area-bolt");

		Config config = new Config();
		config.setNumWorkers(4);
		config.setDebug(true);
		LocalCluster localCluster=new LocalCluster();
		localCluster.submitTopology("area-topology", config, builder.createTopology());

	}
}

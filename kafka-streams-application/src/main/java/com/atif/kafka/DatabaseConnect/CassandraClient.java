package com.atif.kafka.DatabaseConnect;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;

import java.io.File;

/**
 * Created by mars137 on 6/20/17.
 */
public class CassandraClient
{


    private static final Logger LOG = LoggerFactory.getLogger(CassandraClient.class);
    private static final Config config = ConfigFactory.load("cassandra");
    public static void main(String args[]) {
        CassandraConnector connector = new CassandraConnector();

        connector.connect(config.getString("cassandra.node1"), 9042);
        connector.connect(config.getString("cassandra.node2"), 9042);
        connector.connect(config.getString("cassandra.node3"), 9042);

        Session session = connector.getSession();

        KeyspaceRepository sr = new KeyspaceRepository(session);
        sr.useKeyspace("adstreams");




       connector.close();


    }








}

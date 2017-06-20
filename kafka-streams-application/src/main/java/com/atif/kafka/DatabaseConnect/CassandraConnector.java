package com.atif.kafka.DatabaseConnect;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Created by mars137 on 6/20/17.
 */
public class CassandraConnector
{
    private Cluster cluster;
    private KeyspaceRepository schemaRepository;
    private Session session;


    public void connect(String node, Integer port) {
        Cluster.Builder b = Cluster.builder().addContactPoint(node);
        if (port != null) {
            b.withPort(port);
        }
        cluster = b.build();

        session = cluster.connect();
    }

    public Session getSession() {
        return this.session;
    }

    public void close() {
        session.close();
        cluster.close();
    }

   }



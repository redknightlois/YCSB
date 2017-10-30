/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import net.ravendb.abstractions.data.DatabaseDocument;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.connection.IGlobalAdminDatabaseCommands;
import net.ravendb.client.document.DocumentStore;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="http://ceph.org/">RavenDB 3.5</a>.
 *
 * See {@code ravendb35/README.md} for details.
 */
public class RavenDB35Client extends DB {

  public static final String URL_PROPERTY = "ravendb.url";
  public static final String URL_DEFAULT = "http://localhost:10301";

  private boolean isInited = false;
  private IDocumentStore store;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    // Just use the standard connection format URL
    // http://docs.mongodb.org/manual/reference/connection-string/
    // to configure the client.
    String url = props.getProperty(URL_PROPERTY, URL_DEFAULT);

    if (!url.startsWith("http://")) {
      System.err.println("ERROR: Invalid URL: '" + url
          + "'. Must be of the form "
          + "'http://<host>:<port>. "
          + "https://ravendb.net/docs/article-page/3.5/java/start/getting-started");
      System.exit(1);
    }

    // Initialize the database and document store.
    store = new DocumentStore(url, "YCSB").initialize();

    IGlobalAdminDatabaseCommands adminCommand = store.getDatabaseCommands().getGlobalAdmin();

    String[] databases = adminCommand.getDatabaseNames(1024);

    boolean databaseExist = false;

    for (String database : databases) {
      if (database.equalsIgnoreCase("YCSB")) {
        databaseExist = true;
        break;
      }
    }

    if (!databaseExist) {
      DatabaseDocument databaseDocument = new DatabaseDocument();
      databaseDocument.setId("YCSB");

      Map<String, String> settings = new HashMap<>();
      settings.put("Raven/ActiveBundles", "PeriodicExport");
      settings.put("Raven/DataDir", "~\\Databases\\YCSB");
      settings.put("Raven/AnonymousAccess", "Admin");
      settings.put("Raven/StorageEngine", "voron");

      databaseDocument.setSettings(settings);
      adminCommand.createDatabase(databaseDocument);
    }

    isInited = true;
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    if (isInited) {
      // Perform shutdown

      isInited = false;
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    byte[] buffer;

    try {
      // Read the document from the database.
      JsonDocument doc = store.getDatabaseCommands().get(key);

      if (doc != null) {
        // Write the value of the result to the buffer.
        fillMap(result, doc);
        return Status.OK;
      }

      return Status.NOT_FOUND;

    } catch (Exception e) {
      System.err.println(e.toString());

      return Status.ERROR;
    }
  }

  /**
   * Fills the map with the values from the DBObject.
   *
   * @param resultMap
   *          The map to fill/
   * @param obj
   *          The object to copy values from.
   */
  protected void fillMap(Map<String, ByteIterator> resultMap, JsonDocument obj) {
    for (Map.Entry<String, RavenJToken> entry : obj.toJson()) {
      byte[] content = entry.getValue().toString().getBytes(StandardCharsets.UTF_8);
      resultMap.put(entry.getKey(), new ByteArrayByteIterator(content));
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      // We write the document

      RavenJObject obj = new RavenJObject();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        obj.add(entry.getKey(), entry.getValue().toString());
      }

      store.getDatabaseCommands().put(key, null, obj, new RavenJObject());

    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }

    return Status.OK;
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {
      // We remove the document from the database.
      store.getDatabaseCommands().delete(key, null);

    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }

    return Status.OK;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
}

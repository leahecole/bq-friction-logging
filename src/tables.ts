// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';
import {BigQuery} from '@google-cloud/bigquery';
import type * as BigQueryType from '@google-cloud/bigquery';
const projectId = process.env.GCLOUD_PROJECT
// Import the Google Cloud client library and create a client
const bigquery = new BigQuery();
import {randomUUID} from 'crypto';

const RESOURCE_PREFIX = 'bq_friction_logging';
const UUID = randomUUID() 
const datasetId = `${RESOURCE_PREFIX}_datasets_${UUID}`.replace(
  /-/gi,
  '_',
);
const tableId = `${RESOURCE_PREFIX}_tables_${UUID}`.replace(
  /-/gi,
  '_',
);
const schema = [
    {name: 'Name', type: 'STRING', mode: 'REQUIRED'},
    {name: 'Age', type: 'INTEGER'},
    {name: 'Weight', type: 'FLOAT'},
    {name: 'IsMagic', type: 'BOOLEAN'},
  ];
async function createTable() {

    // TODO - update what gets passed to the request
    // Table resources live within datasets, so we need to create a dataset first
    const datasetOptions = {
      location: 'US',
    };
    // TODO - update call and its parameters
    const [dataset] = await bigquery.createDataset(datasetId, datasetOptions);
    console.log(`Dataset ${dataset.id} created.`);
    const tableOptions = {
      schema: schema,
      location: 'US',
    };

    // Create a new table in the dataset
    const [table] = await bigquery
      .dataset(datasetId)
      .createTable(tableId, tableOptions);

    console.log(`Table ${table.id} created.`);

  }
async function listTables(datasetId: string){
    // lists all tables in the dataset
    const [tables] = await bigquery.dataset(datasetId).getTables();
    console.log('Tables:');
    tables.forEach((table: BigQueryType.Table) => console.log(table.id));
}

// Get info about the dataset, update its description
async function updateTable(tableId: string){
    // TODO - update what gets passed to the request
    // TODO - update call(s) and its parameters
     // Retreive current table metadata
    const table = bigquery.dataset(datasetId).table(tableId);
    const [metadata] = await table.getMetadata();

    // Set new table description
    const description = 'New table description.';
    metadata.description = description;
    const [apiResponse] = await table.setMetadata(metadata);
    const newDescription = apiResponse.description;

    console.log(`${tableId} description: ${newDescription}`);

}

  async function deleteTable(tableId: string) {
    // TODO - update what gets passed to the request
    // Create a reference to the existing dataset
    const dataset = bigquery.dataset(datasetId);
    // TODO - update call and its parameters
    // Delete the table
    await dataset.table(tableId).delete();
    console.log(`Table ${tableId} deleted.`);

    // TODO - update call and its parameters
    // Delete the dataset and its contents
    await dataset.delete({force: true});
    console.log(`Dataset ${dataset.id} deleted.`);
  }

// wrap in an async main function so we can make calls in order
async function main(){
    await createTable();
    await listTables(datasetId);
    await updateTable(tableId)
    await deleteTable(tableId)
}
main();




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
import {BigQueryClient} from '@google-cloud/bigquery';
import type * as BigQueryType from '@google-cloud/bigquery';
const projectId = process.env.GCLOUD_PROJECT
// Import the Google Cloud client library and create a client
const bigquery = new BigQueryClient();
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

    // Table resources live within datasets, so we need to create a dataset first
    // Specify the geographic location where the dataset should reside
    const datasetObject: BigQueryType.protos.google.cloud.bigquery.v2.IDataset = {
      datasetReference: {
        datasetId: datasetId,
      },
      location: 'US',
    };

    // Construct the request object.
    const datasetRequest: BigQueryType.protos.google.cloud.bigquery.v2.IInsertDatasetRequest = {
      projectId: projectId,
      dataset: datasetObject,
    };
    // Create a new dataset
    const [dataset] = await bigquery.insertDataset(datasetRequest) as [
      BigQueryType.protos.google.cloud.bigquery.v2.IDataset,
      BigQueryType.protos.google.cloud.bigquery.v2.IInsertDatasetRequest | undefined,
      {} | undefined,
    ];
    console.log(`Dataset ${dataset.id} created.`);

    const tableRequest: BigQueryType.protos.google.cloud.bigquery.v2.IInsertTableRequest = {
      projectId,
      datasetId,
      table: {
        tableReference: {
          projectId,
          datasetId,
          tableId,
        },
        schema: {fields: schema},
        location: 'US',
      },
    };

    // Create a new table in the dataset
    const [table] = await bigquery.insertTable(tableRequest) as [
      BigQueryType.protos.google.cloud.bigquery.v2.ITable,
      BigQueryType.protos.google.cloud.bigquery.v2.IInsertTableRequest | undefined,
      {} | undefined,
    ];

    console.log(`Table ${table.id} created.`);

  }

async function listTables(datasetId: string){
    // lists all tables in the dataset
    const request: BigQueryType.protos.google.cloud.bigquery.v2.IListTablesRequest = {
          projectId,
          datasetId,
        };

    // List all tables in the dataset
    const [tables] = await bigquery.listTables(request) as [
      BigQueryType.protos.google.cloud.bigquery.v2.IListFormatTable[],
      BigQueryType.protos.google.cloud.bigquery.v2.IListTablesRequest | null,
      BigQueryType.protos.google.cloud.bigquery.v2.ITableList,
    ]

    console.log('Tables:');
    //@ts-ignore
    tables.forEach((table: BigQueryType.protos.google.cloud.bigquery.v2.ITable) => console.log(table.tableReference!.tableId));
}

// Get info about the dataset, update its description
async function updateTable(tableId: string){
     // Retreive current table metadata
    
    const getRequest: BigQueryType.protos.google.cloud.bigquery.v2.IGetTableRequest = {
      projectId: projectId,
      datasetId: datasetId,
      tableId: tableId,
    };

    const [table] = await bigquery.getTable(getRequest) as [
      BigQueryType.protos.google.cloud.bigquery.v2.ITable,
      BigQueryType.protos.google.cloud.bigquery.v2.IGetTableRequest | undefined,
      {} | undefined,
    ] ;

    console.log('Table:');
    console.log(table);

    // Set new table description
    const description = 'New table description.';
    const updateRequest = {
      projectId: projectId,
      datasetId: datasetId,
      tableId: tableId,
      table: {
        tableReference: {tableId: tableId},
        description: {value: description},
      },
    };
    const [response] = await bigquery.updateTable(updateRequest) as [
      BigQueryType.protos.google.cloud.bigquery.v2.ITable,
     BigQueryType.protos.google.cloud.bigquery.v2.IUpdateOrPatchTableRequest | undefined,
      {} | undefined,
    ];

    const newDescription = response.description!.value;

    console.log(`${tableId} description: ${newDescription}`);

}

  async function deleteTable(tableId: string) {

    // Delete the table
     const tableRequest: BigQueryType.protos.google.cloud.bigquery.v2.IDeleteTableRequest = {
      projectId: projectId,
      datasetId: datasetId,
      tableId: tableId,
    };

    // Delete the table
    await bigquery.deleteTable(tableRequest);

    console.log(`Table ${tableId} deleted.`);

    // Delete the dataset and its contents
    const deleteRequest: BigQueryType.protos.google.cloud.bigquery.v2.IDeleteDatasetRequest = {
        projectId: projectId,
        datasetId: datasetId,
        deleteContents: true, // Set to true to delete all tables in the dataset
      };
    await bigquery.deleteDataset(deleteRequest);
      console.log(`Dataset ${datasetId} deleted.`);
  }

// wrap in an async main function so we can make calls in order
async function main(){
    await createTable();
    await listTables(datasetId);
    await updateTable(tableId)
    await deleteTable(tableId)
}
main();



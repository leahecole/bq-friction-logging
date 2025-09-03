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
import {randomUUID} from 'crypto';

const projectId = process.env.GCLOUD_PROJECT
const RESOURCE_PREFIX = 'bq_friction_logging';
const datasetId = `${RESOURCE_PREFIX}_datasets_${randomUUID()}`.replace(
  /-/gi,
  '_',
);

const bigquery = new BigQueryClient();

async function createDataset() {

    // Specify the geographic location where the dataset should reside
    const datasetObject: BigQueryType.protos.google.cloud.bigquery.v2.IDataset = {
      datasetReference: {
        datasetId: datasetId,
      },
      location: 'US',
    };

    // Construct the request object.
    const request: BigQueryType.protos.google.cloud.bigquery.v2.IInsertDatasetRequest = {
      projectId: projectId,
      dataset: datasetObject,
    };
    // Create a new dataset
    const [dataset] = await bigquery.insertDataset(request) as [
      BigQueryType.protos.google.cloud.bigquery.v2.IDataset,
      BigQueryType.protos.google.cloud.bigquery.v2.IInsertDatasetRequest | undefined,
      {} | undefined,
    ];
    console.log(`Dataset ${dataset.id} created.`);
  }
async function listDatasets(){
     const request: BigQueryType.protos.google.cloud.bigquery.v2.IListDatasetsRequest = {
      projectId: projectId,
    };
    // List all datasets in the project
    const [datasets] = await bigquery.listDatasets(request) as [
      BigQueryType.protos.google.cloud.bigquery.v2.IListFormatDataset[],
      BigQueryType.protos.google.cloud.bigquery.v2.IListDatasetsRequest | null,
      BigQueryType.protos.google.cloud.bigquery.v2.IDatasetList,
    ];
    console.log('Datasets:');
    datasets.forEach((dataset: BigQueryType.protos.google.cloud.bigquery.v2.IDataset) => console.log(dataset.id));
}

// // Get info about the dataset, update its description
async function updateDataset(datasetId: string){
    const getRequest: BigQueryType.protos.google.cloud.bigquery.v2.IGetDatasetRequest = {
      projectId: projectId,
      datasetId: datasetId,
    };
    const dataset = await bigquery.getDataset(getRequest)
    console.log("Dataset info:")
    console.log(dataset)
    // Set new dataset description
    const description = 'New dataset description.';
    const datasetToUpdate = {
      projectId: projectId,
      datasetId: datasetId,
      datasetReference: {
        datasetId: datasetId,
      },
      description: {value: description},
    };
    // Construct the request object.
    const updateRequest: BigQueryType.protos.google.cloud.bigquery.v2.IUpdateOrPatchDatasetRequest = {
      projectId: projectId,
      datasetId: datasetId,
      dataset: datasetToUpdate,
    };
    const [updateResponse] = await bigquery.updateDataset(updateRequest) as [
      BigQueryType.protos.google.cloud.bigquery.v2.IDataset,
      BigQueryType.protos.google.cloud.bigquery.v2.IUpdateOrPatchDatasetRequest | undefined,
      {} | undefined,
    ];
    const newDescription = updateResponse.description!.value;

    console.log(`${datasetId} description: ${newDescription}`);


}
  async function deleteDataset(datasetId: string) {
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
    await createDataset();
    await listDatasets();
    await updateDataset(datasetId)
    await deleteDataset(datasetId)
}
main();





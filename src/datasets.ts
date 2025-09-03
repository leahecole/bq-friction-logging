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
import type {Dataset} from '@google-cloud/bigquery';
import {randomUUID} from 'crypto';

const projectId = process.env.GCLOUD_PROJECT
const RESOURCE_PREFIX = 'bq_friction_logging';
const datasetId = `${RESOURCE_PREFIX}_datasets_${randomUUID()}`.replace(
  /-/gi,
  '_',
);
const bigquery = new BigQuery();

async function createDataset() {
    // Specify the geographic location where the dataset should reside
    const options = {
      location: 'US',
    };
    // Create a new dataset
    const [dataset] = await bigquery.createDataset(datasetId, options);
    console.log(`Dataset ${dataset.id} created.`);
  }
async function listDatasets(){
    // Lists all datasets
    const [datasets] = await bigquery.getDatasets({projectId});
    console.log('Datasets:');
    datasets.forEach((dataset: Dataset) => console.log(dataset.id));
}

// Get info about the dataset, update its description
async function updateDataset(datasetId: string){
    const dataset = bigquery.dataset(datasetId)
    const [datasetInfo] = await dataset.getMetadata();
    console.log("Dataset info:")
    console.log(datasetInfo)
    // Set new dataset description
    const description = 'New dataset description.';
    datasetInfo.description = description;

    const [apiResponse] = await dataset.setMetadata(datasetInfo);
    const newDescription = apiResponse.description;

    console.log(`${datasetId} description: ${newDescription}`);


}
  async function deleteDataset(datasetId: string) {

    // Create a reference to the existing dataset
    const dataset = bigquery.dataset(datasetId);

    // Delete the dataset and its contents
    await dataset.delete({force: true});
    console.log(`Dataset ${dataset.id} deleted.`);
  }
// wrap in an async main function so we can make calls in order
async function main(){
    await createDataset();
    await listDatasets();
    await updateDataset(datasetId)
    await deleteDataset(datasetId)
}
main();





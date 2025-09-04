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
// TODO - update import
import {BigQuery} from '@google-cloud/bigquery';
import type * as BigQueryType from '@google-cloud/bigquery';
import {randomUUID} from 'crypto';

const projectId = process.env.GCLOUD_PROJECT
// Import the Google Cloud client library and create a client
// TODO - update client initialization
const bigquery = new BigQuery();



const RESOURCE_PREFIX = 'bq_friction_logging';
const UUID = randomUUID() 
const datasetId = `${RESOURCE_PREFIX}_datasets_${UUID}`.replace(
  /-/gi,
  '_',
);
const routineId = `${RESOURCE_PREFIX}_routines_${UUID}`.replace(
  /-/gi,
  '_',
);



async function createRoutine() {

    // TODO - update what gets passed to the request
    // Table resources live within datasets, so we need to create a dataset first
    const datasetOptions = {
      location: 'US',
    };
    // TODO - update call and its parameters
    const [dataset] = await bigquery.createDataset(datasetId, datasetOptions);
    console.log(`Dataset ${dataset.id} created.`);
    // Create routine reference
    let routine = dataset.routine(routineId);

    const config = {
      arguments: [
        {
          name: 'x',
          dataType: {
            typeKind: 'INT64',
          },
        },
      ],
      definitionBody: 'x * 3',
      routineType: 'SCALAR_FUNCTION',
      returnType: {
        typeKind: 'INT64',
      },
    };

    // Make API call
    [routine] = await routine.create(config);

    console.log(`Routine ${routineId} created.`);

  }


async function listRoutines(datasetId: string){
// List all routines
    const [routines] = await bigquery.dataset(datasetId).getRoutines();
    console.log('Routines:');
    routines.forEach((routine: BigQueryType.Routine) => console.log(routine.id));
    // Show us what's going on there - does it have a friendly name, does it have a description, can you change it
}



async function updateRoutine(routineId: string){
    // TODO - update what gets passed to the request
    // TODO - update call(s) and its parameters
     // Retreive current routine metadata
    const routine = bigquery.dataset(datasetId).routine(routineId);
    const [metadata] = await routine.getMetadata();

    // Set new routine description
    const description = 'New routine description.';
    metadata.description = description;
    const [apiResponse] = await routine.setMetadata(metadata);
    const newDescription = apiResponse.description;

    console.log(`${routineId} description: ${newDescription}`);

}

  async function deleteRoutine(routineId: string) {
    // TODO - update what gets passed to the request
    // Create a reference to the existing dataset
    const dataset = bigquery.dataset(datasetId);
    // TODO - update call and its parameters
    // Delete the routine
       // Create routine reference
    let routine = dataset.routine(routineId);

    // Make API call
    await routine.delete();

    console.log(`Routine ${routineId} deleted.`);
    // TODO - update call and its parameters
    // Delete the dataset and its contents
    await dataset.delete({force: true});
    console.log(`Dataset ${dataset.id} deleted.`);
  }

// wrap in an async main function so we can make calls in order
async function main(){
    await createRoutine();
    await listRoutines("nodejs_samples_tests_datasets_f8b28ba7_75ae_4583_adce_d10a97cb5940")
    await updateRoutine(routineId)
    await deleteRoutine(routineId)
}
main();

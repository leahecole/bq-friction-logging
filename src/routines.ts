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
// Import the Google Cloud client library and create a client
const bigquery = new BigQueryClient();



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

    // Routine resources live within datasets, so we need to create a dataset first
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
    const routine: BigQueryType.protos.google.cloud.bigquery.v2.IRoutine = {
      routineReference: {
        projectId,
        datasetId,
        routineId,
      },
      arguments: [
        {
          name: 'x',
          dataType: {typeKind: 'INT64'},
        },
      ],
      definitionBody: 'x * 3',
      routineType: 'SCALAR_FUNCTION',
      returnType: {typeKind: 'INT64'},
    };

    const insertRequest: BigQueryType.protos.google.cloud.bigquery.v2.IInsertRoutineRequest = {
      projectId: projectId,
      datasetId: datasetId,
      routine: routine,
    };
    // Make API call
    const [response] = await bigquery.insertRoutine(insertRequest) as [
      BigQueryType.protos.google.cloud.bigquery.v2.IRoutine,
      BigQueryType.protos.google.cloud.bigquery.v2.IInsertRoutineRequest | undefined,
      {} | undefined,
    ];

    console.log(`Routine ${response.routineReference!.routineId} created.`);

  }


async function listRoutines(datasetId: string){
// List all routines
 const listRequest = {
      projectId: projectId,
      datasetId: datasetId,
    };
    // List all routines in the dataset
    const [routines] = await bigquery.listRoutines(listRequest) as [
      BigQueryType.protos.google.cloud.bigquery.v2.IRoutine[],
      BigQueryType.protos.google.cloud.bigquery.v2.IListRoutinesRequest | null,
      BigQueryType.protos.google.cloud.bigquery.v2.IListRoutinesResponse,
    ];

    console.log('Routines:');
    routines.forEach(routine =>
      console.log(routine.routineReference!.routineId),
    );
}



async function updateRoutine(routineId: string){

     // Retreive current routine metadata
    const getRequest = {
      projectId: projectId,
      datasetId: datasetId,
      routineId: routineId,
    };
    // Create routine reference and make API call
    const [routine] = await bigquery.getRoutine(getRequest) as  [
      BigQueryType.protos.google.cloud.bigquery.v2.IRoutine,
      BigQueryType.protos.google.cloud.bigquery.v2.IGetRoutineRequest | undefined,
      {} | undefined,
    ];
    console.log(`Routine ${routine}`)


    // Set new routine description
    const description = 'New routine description.';
    routine.description = description;
   const updateRequest = {
      projectId: projectId,
      datasetId: datasetId,
      routineId: routineId,
      routine: routine,
    };
    // Make API call
    const [apiResponse] = await bigquery.updateRoutine(updateRequest) as [
      BigQueryType.protos.google.cloud.bigquery.v2.IRoutine,
      BigQueryType.protos.google.cloud.bigquery.v2.IUpdateRoutineRequest | undefined,
      {} | undefined,
    ];
    const newDescription = apiResponse.description;

    console.log(`${routineId} description: ${newDescription}`);

}

  async function deleteRoutine(routineId: string) {
    const routineRequest = {
      projectId: projectId,
      datasetId: datasetId,
      routineId: routineId,
    };
    // Make API call
    await bigquery.deleteRoutine(routineRequest);

    console.log(`Routine ${routineId} deleted.`);
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
    await createRoutine();
    await listRoutines(datasetId)
    await updateRoutine(routineId)
    await deleteRoutine(routineId)
}
main();
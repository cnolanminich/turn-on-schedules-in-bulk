from dagster import Definitions, load_assets_from_modules, AssetSelection, define_asset_job, op, OpExecutionContext, job, asset, asset_check, ScheduleDefinition
import requests
from datetime import datetime
from turn_on_schedules_in_bulk import assets
from turn_on_schedules_in_bulk.blueprints import loader


# can be modified to work with auth in dagster+
graphql_endpoint = "http://localhost:3000/graphql"  # Adjust the URL as needed
code_location_name = "turn_on_schedules_in_bulk"  # Adjust the code location name as needed

all_assets = load_assets_from_modules([assets])

def get_all_schedules(graphql_endpoint, code_location_name):
        # Define the GraphQL query
    get_all_schedules_query = f"""
    query SchedulesQuery {{
  schedulesOrError(repositorySelector: {{ repositoryName: "__repository__", repositoryLocationName: "{code_location_name}" }}) {{
    ... on Schedules {{
      results {{
        id
        name
        cronSchedule
        executionTimezone
        tags{{
          key
          value
        }}
        scheduleState {{
          id
          status
        }}
    }}
    }}
    }}
    }}
    """
    # Send the request to the GraphQL endpoint
    schedules_response = requests.post(
        graphql_endpoint,
        json={'query': get_all_schedules_query}
    )
    return schedules_response

@op
def turn_on_asset_grouping_1_schedules(context: OpExecutionContext) -> None:
    graphql_endpoint = "http://localhost:3000/graphql"  # Adjust the URL as needed
    code_location_name = "turn_on_schedules_in_bulk"  # Adjust the code location name as needed

    # Send the request to the GraphQL endpoint
    schedules_response = get_all_schedules(graphql_endpoint, code_location_name) 
    # Filter the data
    filtered_ids = [
        item['name']
        #item['id']
        for item in schedules_response.json()['data']['schedulesOrError']['results']
        if any(tag['key'] == 'schedule-type' and tag['value'] == 'asset_grouping_1' for tag in item.get('tags', []))
    ]
    
    for schedule_name in filtered_ids:
        activate_schedule_mutation = f"""
            mutation StartSchedule {{
        startSchedule(scheduleSelector: {{  
                repositoryName: "__repository__", 
                repositoryLocationName: "{code_location_name}", 
                scheduleName: "{schedule_name}"}}) {{
            __typename
            ... on ScheduleStateResult {{
            scheduleState {{
                id
                status
            }}
            }}
        }}
        }}
        """
        # Send the request to the GraphQL endpoint
        activate_schedules_response = requests.post(
            graphql_endpoint,
            json={'query': activate_schedule_mutation}
        )
        context.log.info(activate_schedules_response.json())
        
    schedules_responses_updated = get_all_schedules(graphql_endpoint, code_location_name)
    # Check for errors in the response
    if schedules_response.status_code == 200:
        context.log.info(f"GraphQL query successful, Schedules here: {schedules_responses_updated.json()}")
    else:
        context.log.error(f"GraphQL query failed: {schedules_responses_updated.text}")

@op
def turn_off_expired_schedules(context: OpExecutionContext) -> None:
    graphql_endpoint = "http://localhost:3000/graphql"  # Adjust the URL as needed
    code_location_name = "turn_on_schedules_in_bulk"  # Adjust the code location name as needed
    schedules_response = get_all_schedules(graphql_endpoint, code_location_name)
    # Filter the data
    filtered_ids = [
        #item['name']
        item['id']
        for item in schedules_response.json()['data']['schedulesOrError']['results']
        if any(tag['key'] == 'schedule-expiration' and datetime.strptime(tag['value'], '%Y-%m-%d').date() <= datetime.today().date() for tag in item.get('tags', []))
    ]
    
    # Send the request to the GraphQL endpoint
    schedules_response = get_all_schedules(graphql_endpoint, code_location_name)

    context.log.info(f"All Schedules: {schedules_response.json()}")
    for schedule_id in filtered_ids:
        activate_schedule_mutation = f"""
            mutation StopSchedule {{
        stopRunningSchedule( id: "{schedule_id}") {{
            __typename
            ... on ScheduleStateResult {{
            scheduleState {{
                id
                status
            }}
            }}
            ... on PythonError {{
                message
                stack
        }}
        }}
        }}
        """
        # Send the request to the GraphQL endpoint
        activate_schedules_response = requests.post(
            graphql_endpoint,
            json={'query': activate_schedule_mutation}
        )
        context.log.info(f"Updated Schedules: {schedule_id} {activate_schedules_response.json()}")
        
    schedules_responses_updated = get_all_schedules(graphql_endpoint, code_location_name)
    # Check for errors in the response
    if schedules_response.status_code == 200:
        context.log.info(f"GraphQL query successful: {schedules_responses_updated.json()}")
    else:
        context.log.error(f"GraphQL query failed: {schedules_responses_updated.text}")


@job 
def turn_off_expired_schedules_job():
    turn_off_expired_schedules()
    
@job 
def turn_on_asset_grouping_1_schedules_job():
    turn_on_asset_grouping_1_schedules()
    


defs = Definitions.merge(loader.load_defs(), Definitions(
    assets=[*all_assets],
    jobs=[turn_off_expired_schedules_job, turn_on_asset_grouping_1_schedules_job],
    )
)
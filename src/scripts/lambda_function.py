# lambda_function.py
import json
import subprocess
import os

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event, indent=2)}")

    try:
        print("Executing refresh.sh...")
        result = subprocess.run(["./src/scripts/refresh.sh"],
                                capture_output=True, 
                                text=True,          
                                check=True,         
                                env=os.environ.copy() 
                               )

        print("Shell script stdout:")
        print(result.stdout)
        if result.stderr:
            print("Shell script stderr:")
            print(result.stderr)

        print("Shell script executed successfully.")

        return {
            'statusCode': 200,
            'body': json.dumps('Shell script and Python scripts executed successfully!')
        }

    except subprocess.CalledProcessError as e:
        print(f"Shell script failed with error code {e.returncode}")
        print(f"Command: {e.cmd}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error executing shell script: {e.stderr}')
        }
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'An unexpected error occurred: {str(e)}')
        }
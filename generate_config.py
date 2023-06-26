import configparser
import os
import json

def main():
    '''
    Use `terraform_output.json` and `terraform.tvars.json` files in order to generate a config 
    that stores all necessary AWS variables in order to connect to S3 and Redshift cluster
    '''
    
    # Get the home path string
    home = os.path.expanduser('~')

    # Import values from terraform_output.json
    with open(home + '/github-repos/de-with-aws/terraform/terraform_output.json') as f:
        data = json.load(f)
        DWH_ROLE_ARN = data['redshift_iam_arn']['value']
        DWH_ENDPOINT = data['cluster_endpoint']['value']
        # Remove port form endpoint string
        DWH_ENDPOINT = DWH_ENDPOINT.split(':')[0]

    # Import values from terraform.tvars.json
    with open(home + '/github-repos/de-with-aws/terraform/terraform.tvars.json') as f:
        data = json.load(f)
        DWH_DB = data['redshift_database_name']
        DWH_DB_USER = data['redshift_admin_username']
        DWH_DB_PASSWORD = data['redshift_admin_password']

    config = configparser.ConfigParser()
    # Retain uppercase for options
    config.optionxform = str

    # CLUSTER section
    config.add_section("CLUSTER")
    
    config.set('CLUSTER', 'HOST', DWH_ENDPOINT)
    config.set('CLUSTER', 'DB_NAME', DWH_DB)
    config.set('CLUSTER', 'DB_USER', DWH_DB_USER)
    config.set('CLUSTER', 'DB_PASSWORD', DWH_DB_PASSWORD)
    # Set default Redshifr port
    config.set('CLUSTER', 'DB_PORT', '5439')
    
    # IAM_ROLE section
    config.add_section("IAM_ROLE")
    
    config.set('IAM_ROLE', 'ARN', f"'{DWH_ROLE_ARN}'")
    
    # S3 paths section
    config.add_section("S3")
    
    config.set('S3', 'LOG_DATA', "'s3://udacity-dend/log_data'")
    config.set('S3', 'LOG_JSONPATH', "'s3://udacity-dend/log_json_path.json'")
    config.set('S3', 'SONG_DATA', "'s3://udacity-dend/song_data'")
    
    with open("dwh.cfg", 'w') as f:
        config.write(f)
        
if __name__ == '__main__':
    main()
AWS Access
==========

PynamoDB uses botocore to interact with the DynamoDB API. Thus, any method of configuration supported by ``botocore`` works with PynamoDB.
For local development the use of environment variables such as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
is probably preferable. You can of course use IAM users, as recommended by AWS. In addition
`EC2 roles <http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html>`_ will work as well and
would be recommended when running on EC2.

As for the permissions granted via IAM, many tasks can be carried out by PynamoDB. So you should construct your
policies as required, see the
`DynamoDB <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/UsingIAMWithDDB.html>`_ docs for more
information.

If for some reason you can't use conventional AWS configuration methods, you can set the credentials in the Model Meta class:

.. code-block:: python

    from pynamodb.models import Model

    class MyModel(Model):
        class Meta:
            aws_access_key_id = 'my_access_key_id'
            aws_secret_access_key = 'my_secret_access_key'
            aws_session_token = 'my_session_token' # Optional, only for temporary credentials like those received when assuming a role

If you need to access DynamoDB passing for a specific AWS Role and so you need to perform an assume-role operation on your Role ARN you can configure it in the Model Meta class:

.. code-block:: python

    from pynamodb.models import Model

    class MyModel(Model):
        class Meta:
            aws_sts_role_arn='arn:aws:iam::1234567:role/my-aws-role'
            aws_sts_role_session_name='MySession' # Optional, by default it is PynamoDB
            aws_sts_session_expiration=3600 # Optional, it is the session token duration in seconds. Default is 3600

Note that your environment variables credentials, or in case, credentials setted in the Model Meta class, need to have grant to perform the assume-role operation.

Finally, see the `AWS CLI documentation <http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-installing-credentials>`_
for more details on how to pass credentials to botocore.
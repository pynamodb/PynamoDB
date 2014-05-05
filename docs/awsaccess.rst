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

Finally, see the `AWS CLI documentation <http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-installing-credentials>`_
for more details on how to pass credentials to botocore.
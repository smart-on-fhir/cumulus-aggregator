"""The following is based on the AWS lambda authorizer blueprint with modifications
https://github.com/awslabs/aws-apigateway-lambda-authorizer-blueprints/blob/d49eb6a1e2ae4a01688fb2bc861cd463d3ac5eb0/blueprints/python/api-gateway-authorizer-python.py
"""

# pylint: disable=invalid-name,pointless-string-statement
from __future__ import print_function

import json
import re

import bcrypt


def lambda_handler(event, context):  # pylint: disable=unused-argument
    # ---- aggregator specific logic
    with open("src/handlers/site_data/auth.json", encoding="utf-8") as auth:
        user_db = json.load(auth)
    try:
        user_details = user_db[event["headers"]["user"]]
        if not bcrypt.checkpw(
            event["headers"]["Authorization"].encode("utf-8"),
            user_details["secret"].encode("utf-8"),
        ):
            raise Exception
    except Exception:
        raise Exception("Unauthorized")  # pylint: disable=raise-missing-from

    principalId = event["headers"]["user"]

    # ---- end aggregator specific logic

    tmp = event["methodArn"].split(":")
    apiGatewayArnTmp = tmp[5].split("/")
    awsAccountId = tmp[4]

    policy = AuthPolicy(principalId, awsAccountId)
    policy.restApiId = apiGatewayArnTmp[0]
    policy.region = tmp[3]
    policy.stage = apiGatewayArnTmp[1]

    policy.allowMethod(HttpVerb.POST, "/")

    authResponse = policy.build()

    return authResponse


# The remaining functions/methods are all part of the original blueprint linked above,
# with accomodations for our linting, but should otherwise not be modified unless
# you're dramatically changing the behavior for some reason


class HttpVerb:
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    HEAD = "HEAD"
    DELETE = "DELETE"
    OPTIONS = "OPTIONS"
    ALL = "*"


class AuthPolicy(object):  # pylint: disable=missing-class-docstring
    awsAccountId = ""
    """The AWS account id the policy will be generated for. This is used to
    create the method ARNs."""
    principalId = ""
    version = "2012-10-17"
    """The policy version used for the evaluation. This should always be '2012-10-17'"""
    pathRegex = r"^[/.a-zA-Z0-9-\*]+$"  # pylint: disable=anomalous-backslash-in-string
    """The regular expression used to validate resource paths for the policy"""

    """these are the internal lists of allowed and denied methods. These are lists
    of objects and each object has 2 properties: A resource ARN and a nullable
    conditions statement.
    the build method processes these lists and generates the approriate
    statements for the final policy"""
    allowMethods = []
    denyMethods = []

    restApiId = "<<restApiId>>"
    """ Replace the placeholder value with a default API Gateway API id to be used in
    the policy. Beware of using '*' since it will not simply mean any API Gateway API
    id, because stars will greedily expand over '/' or other separators. See
    https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_resource.html
    for more details. """

    region = "<<region>>"
    """ Replace the placeholder value with a default region to be used in the policy.
    Beware of using '*' since it will not simply mean any region, because stars will
    greedily expand over '/' or other separators. See
    https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_resource.html
    for more details. """

    stage = "<<stage>>"
    """ Replace the placeholder value with a default stage to be used in the policy.
    Beware of using '*' since it will not simply mean any stage, because stars will
    greedily expand over '/' or other separators. See
    https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_resource.html
    for more details. """

    def __init__(self, principal, awsAccountId):
        self.awsAccountId = awsAccountId
        self.principalId = principal
        self.allowMethods = []
        self.denyMethods = []

    def _addMethod(self, effect, verb, resource, conditions):
        """Adds a method to the internal lists of allowed or denied methods. Each
        object in the internal list contains a resource ARN and a condition statement.
        The condition statement can be null."""
        if verb != "*" and not hasattr(HttpVerb, verb):
            raise NameError(
                "Invalid HTTP verb " + verb + ". Allowed verbs in HttpVerb class"
            )
        resourcePattern = re.compile(self.pathRegex)
        if not resourcePattern.match(resource):
            raise NameError(
                "Invalid resource path: "
                + resource
                + ". Path should match "
                + self.pathRegex
            )

        if resource[:1] == "/":
            resource = resource[1:]

        resourceArn = (
            "arn:aws:execute-api:"
            + self.region
            + ":"
            + self.awsAccountId
            + ":"
            + self.restApiId
            + "/"
            + self.stage
            + "/"
            + verb
            + "/"
            + resource
        )

        if effect.lower() == "allow":
            self.allowMethods.append(
                {"resourceArn": resourceArn, "conditions": conditions}
            )
        elif effect.lower() == "deny":
            self.denyMethods.append(
                {"resourceArn": resourceArn, "conditions": conditions}
            )

    def _getEmptyStatement(self, effect):
        """Returns an empty statement object prepopulated with the correct action and
        the desired effect."""
        statement = {
            "Action": "execute-api:Invoke",
            "Effect": effect[:1].upper() + effect[1:].lower(),
            "Resource": [],
        }

        return statement

    def _getStatementForEffect(self, effect, methods):
        """This function loops over an array of objects containing a resourceArn and
        conditions statement and generates the array of statements for the policy."""
        statements = []

        if len(methods) > 0:
            statement = self._getEmptyStatement(effect)

            for curMethod in methods:
                if curMethod["conditions"] is None or len(curMethod["conditions"]) == 0:
                    statement["Resource"].append(curMethod["resourceArn"])
                else:
                    conditionalStatement = self._getEmptyStatement(effect)
                    conditionalStatement["Resource"].append(curMethod["resourceArn"])
                    conditionalStatement["Condition"] = curMethod["conditions"]
                    statements.append(conditionalStatement)

            statements.append(statement)

        return statements

    def allowAllMethods(self):
        """Adds a '*' allow to the policy to authorize access to all methods of
        an API"""
        self._addMethod("Allow", HttpVerb.ALL, "*", [])

    def denyAllMethods(self):
        """Adds a '*' allow to the policy to deny access to all methods of an API"""
        self._addMethod("Deny", HttpVerb.ALL, "*", [])

    def allowMethod(self, verb, resource):
        """Adds an API Gateway method (Http verb + Resource path) to the list of allowed
        methods for the policy"""
        self._addMethod("Allow", verb, resource, [])

    def denyMethod(self, verb, resource):
        """Adds an API Gateway method (Http verb + Resource path) to the list of denied
        methods for the policy"""
        self._addMethod("Deny", verb, resource, [])

    def allowMethodWithConditions(self, verb, resource, conditions):
        """Adds an API Gateway method (Http verb + Resource path) to the list of allowed
        methods and includes a condition for the policy statement. More on AWS policy
        conditions here:
        http://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements.html#Condition"""  # noqa: E501
        self._addMethod("Allow", verb, resource, conditions)

    def denyMethodWithConditions(self, verb, resource, conditions):
        """Adds an API Gateway method (Http verb + Resource path) to the list of denied
        methods and includes a condition for the policy statement. More on AWS policy
        conditions here:
        http://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements.html#Condition"""  # noqa: E501
        self._addMethod("Deny", verb, resource, conditions)

    def build(self):
        """Generates the policy document based on the internal lists of allowed and
        denied conditions. This will generate a policy with two main statements for
        the effect: one statement for Allow and one statement for Deny.
        Methods that includes conditions will have their own statement in the policy."""
        if (self.allowMethods is None or len(self.allowMethods) == 0) and (
            self.denyMethods is None or len(self.denyMethods) == 0
        ):
            raise NameError("No statements defined for the policy")

        policy = {
            "principalId": self.principalId,
            "policyDocument": {"Version": self.version, "Statement": []},
        }

        policy["policyDocument"]["Statement"].extend(
            self._getStatementForEffect("Allow", self.allowMethods)
        )
        policy["policyDocument"]["Statement"].extend(
            self._getStatementForEffect("Deny", self.denyMethods)
        )

        return policy
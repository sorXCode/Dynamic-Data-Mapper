from flask import Flask, request
from flask_restful import reqparse, abort, Api, Resource
from database import create_table, update_table, read_table


app = Flask(__name__)
api = Api(app)


# def abort_if_providerId_doesnt_exist(providerId):

#     if providerId not in providerIds:
#         abort(404, message="providerId {} doesn't exist".format(providerId))


# DataCreation
class DataCreator(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('providerId', required=True, type=int)
    parser.add_argument('fields', required=True, action='append')

    def post(self):
        args = self.parser.parse_args()
        print(args)
        providerId = str(args["providerId"])
        fields = args['fields']
        create_table(table_name=providerId, columns=fields)
        return args, 201


class DataLoader(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('providerId', required=True, type=int)
    parser.add_argument('data', required=True, type=dict, action='append')

    def post(self):
        args = self.parser.parse_args()
        print(args)
        providerId = str(args["providerId"])
        data = args['data']
        for data_row in data:
            status = update_table(table_name=providerId, data=data_row)
        if status==None:
            return args, 201
        else:
            return "ERROR IN POST DATA", 211


class DataRetriever(Resource):

    def get(self, providerId, *args, **kwargs):
        # r = list(request.args.items())
        # print(r)
        return read_table(table_name=str(providerId))


api.add_resource(DataCreator, '/create', endpoint='create')
api.add_resource(DataLoader, '/load', endpoint='load')
api.add_resource(DataRetriever, '/filter/<int:providerId>',
                 endpoint='retrieve')


if __name__ == '__main__':
    app.run(debug=True)

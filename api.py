from flask import Flask, request
from flask_restful import reqparse, abort, Api, Resource
from database import create_table, update_table, read_table


app = Flask(__name__)
api = Api(app)

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
        providerId = str(args["providerId"])
        data = args['data']

        status = update_table(table_name=providerId, data=data)
        if status == None:
            return args, 201
        else:
            return "ERROR IN POST DATA", 211


class DataRetriever(Resource):

    def get(self, providerId, *args, **kwargs):
        queries = list(request.args.items())
        if len(queries) > 0:
            operators = {
                'eqc': 'ilike',
                'eq': '==',
                'lt': '<',
                'gt': '>'
            }
            spec = list()
            for query in queries:
                spec.append(dict())
                operator = operators.get(query[1].split(':')[0])
                spec[-1]['field'] = query[0]
                spec[-1]['op'] = operator
                spec[-1]['value'] = f"%{query[1].split(':')[1]}%" if operator == 'ilike' else query[1].split(':')[
                    1]

            return read_table(table_name=str(providerId), filter_spec=spec)
        return read_table(table_name=str(providerId))


api.add_resource(DataCreator, '/create', endpoint='create')
api.add_resource(DataLoader, '/load', endpoint='load')
api.add_resource(DataRetriever, '/filter/<int:providerId>',
                 endpoint='retrieve')


if __name__ == '__main__':
    app.run(debug=True)

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Schema;

namespace scheme_validator.Controllers
{
    
    public class JsonController : Controller
    {
        readonly JSchema schema = JSchema.Parse(@"{
    '$schema': 'http://json-schema.org/draft-04/schema#',
    'id': 'Validation Schema',
    'description': 'A schema that validates JSON before writing data in Kafka',
    'type': 'object',
    'properties': {
        'Service': {
            'type': 'object',
            'properties': {
                'ID': {
                    'type': 'string'
                },
                'Version': {
                    'type': 'string'
                }
            },
            'required': [
                'ID',
                'Version'
            ]
        },
        'Customer': {
            'type': 'object',
            'properties': {
                'ID': {
                    'type': 'string'
                },
                'Name': {
                    'type': 'string'
                },
                'Surname': {
                    'type': 'string'
                },
                'Email': {
                    'type': 'string'
                }
            },
            'required': [
                'ID',
                'Name',
                'Surname',
                'Email'
            ]
        }
    },
    'required': [
        'Service',
        'Customer'
    ]
}");


        [Route("/")]
        [HttpPost]
        public string Post([FromBody]JObject req)
        {
            return req.IsValid(schema) ? "valid" : "invalid";
        }
    }
}
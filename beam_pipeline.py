import argparse
import logging

import apache_beam as beam
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec


### Functions and classes

def parse_csv(element):
    row = element.split(',')
    return {
        'sex': row[0],
        'length': float(row[1]),
        'diameter': float(row[2]),
        'height': float(row[3]),
        'whole_weight': float(row[4]),
        'shucked_weight': float(row[5]),
        'viscera_weight': float(row[6]),
        'shell_weight': float(row[7]),
        'rings': int(row[8]),
    }

def convert_to_csv(element, headers):
    return  ','.join([str(element[key]) for key in headers])


class HumidityDoFn(beam.DoFn):

    def process(self, element):
        element['shell_humidity_weight'] = element['whole_weight'] - element['shucked_weight'] - element['shell_weight']
        yield element


class IndexDoFn(beam.DoFn):

    INDEX_STATE = ReadModifyWriteStateSpec(name='index', coder=beam.coders.VarIntCoder())

    def process(self, element, index=beam.DoFn.StateParam(INDEX_STATE)):
        current_index = index.read() or 0
        element[1]['index'] = current_index
        yield element
        index.write(current_index + 1)


class IndexWriteToCsvPTransform(beam.PTransform):

  def __init__(self, options):
    super().__init__()
    self.key = options.get('key')
    self.filename = options.get('filename')
    self.headers = options.get('headers')
    self.output_path = options.get('output_path')

  def expand(self, pcoll):
    return (
        pcoll
        | 'Switch to key-value' >> beam.Map(lambda x: (x.get(self.key, b''), x))
        | 'Assign key' >> beam.ParDo(IndexDoFn())
        | 'Revert key-value' >> beam.Values()
        | 'Convert json -> csv' >> beam.Map(lambda x: convert_to_csv(x, self.headers))
        | 'Write to csv' >> beam.io.WriteToText("%s-%s" % (self.output_path, self.filename),
                    file_name_suffix='.csv',
                    header=','.join(self.headers))
    )



### Main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(
        description='Apache Beam Pipeline')

    parser.add_argument('--input_path', required=True,
                        help='Input path')
    parser.add_argument('--output_path', required=True,
                        help='Output path')

    opts = parser.parse_args()


    input_path = opts.input_path
    output_path = opts.output_path
    headers = ['index', 'sex', 'length', 'diameter', 'height', 'whole_weight', 'shucked_weight', 'viscera_weight', 'shell_weight', 'rings']

    options = {
        'branch1' : {
            'key': 'rings',
            'filename': 'infants_with_more_than_14_rings',
            'output_path': output_path,
            'headers': headers,
            'rings': 14,
        },
        'branch2': {
            'key': False,
            'filename': 'males_heavy_and_short',
            'output_path': output_path,
            'headers': headers,
            'whole_weight': 0.4,
            'length': 0.5,
        },
        'branch3': {
            'key': False,
            'filename': 'shell_humidity',
            'output_path': output_path,
            'headers': headers + ['shell_humidity_weight'],
        }
    }

    # Create the pipeline
    p = beam.Pipeline()

    input = (p | 'Read' >> beam.io.ReadFromText(input_path, skip_header_lines=1)
               | 'Parse .csv' >> beam.Map(parse_csv)
             )

    (input | 'Filter Ring' >> beam.Filter(lambda x: x['rings'] >= options['branch1']['rings'])
           | 'Index and Write Branch 1' >> IndexWriteToCsvPTransform(options['branch1'])
     )

    (input | 'Filter Whole Weight & Length' >> beam.Filter(lambda x: x['whole_weight'] > options['branch2']['whole_weight'] and x['length'] < options['branch2']['length'])
           | 'Index and Write Branch 2' >> IndexWriteToCsvPTransform(options['branch2'])
     )

    (input | 'Compute shell_humidity_weight' >> beam.ParDo(HumidityDoFn())
           | 'Filter Humidity' >> beam.Filter(lambda x: x['shell_humidity_weight'] > 0)
           | 'Index and Write Branch 3' >> IndexWriteToCsvPTransform(options['branch3'])
     )

    logging.info("Building pipeline ...")

    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

import argparse
from abc import abstractmethod
from decimal import Decimal
import csv
import sys
import time
import psycopg2
from psycopg2.extras import execute_values
import pprint
import logging

# Configure CSV reader to handle large fields
csv.field_size_limit(sys.maxsize)


class TransformHelper:
    @staticmethod
    def transform_empty_to_none(d, fields):
        for key, val in d.items():
            if key in fields:
                d[key] = None if val == '' else val

    @staticmethod
    def transform_invalid_booleans(d, fields, true_values=['y', 't']):
        for key, val in d.items():
            if key in fields:
                d[key] = True if val.lower() in true_values else False

    @staticmethod
    def transform_question_mark_to_none(d, fields):
        for key, val in d.items():
            if key in fields:
                d[key] = None if val == '?' else val


class BaseHandler:
    def __init__(self, connection, path, valid_keys, table_name=None):
        self.connection = connection
        self.path = path
        self.table_name = table_name
        self.valid_keys = valid_keys

    @staticmethod
    @abstractmethod
    def dictify(row) -> dict[str, any]:
        pass

    @abstractmethod
    def execute(self, cursor, entities) -> None:
        pass

    def batch_insert(self, entities) -> None:
        with self.connection.cursor() as cursor:
            try:
                self.execute(cursor, entities)
                # logging.info(f"Succesfully inserted {len(entities)} rows")
            except Exception as error:
                logging.error("Error inserting: %s", error)
                logging.warning("Rolling back due to error.")
                self.connection.rollback()
            finally:
                cursor.close()

    def handle(self, batch_size=10000, skip_fkey_validation=False, no_commit=True):
        # Open a file reader to our dataset
        logging.debug("Reading file %s", self.path)
        if skip_fkey_validation:
            logging.warning("Skipping foreign key validation")
        with open(self.path, encoding='UTF-8') as file:
            # Configure our reader to use tab delimiters and no quotations
            rows = csv.reader(file, delimiter="\t", quoting=csv.QUOTE_NONE)
            num_rows = 0
            start = time.time()
            entities = []

            # Pop the header of our TSV
            next(rows)

            for row in rows:
                d = self.dictify(row)
                if not skip_fkey_validation:
                    if d['occurrence_id'] in self.valid_keys:
                        entities.append(d)
                else:
                    entities.append(d)

                num_rows += 1

                if len(entities) >= batch_size:
                    self.batch_insert(entities)
                    entities = []

                if not no_commit:
                    self.connection.commit()

            # Insert entities if there are remaining entities
            if len(entities) > 0:
                self.batch_insert(entities)

            logging.info(
                "Successfully inserted %s records %s (%fs)",
                num_rows,
                '' if not self.table_name else f'into {self.table_name}',
                time.time() - start
            )


class OccurencesHandler(BaseHandler):
    def __init__(self, connection, valid_keys, path="corpus/occurrences.txt"):
        super().__init__(connection, path, valid_keys, "corpus_occurrences")

    # Batch insert entities into corpus_occurrences
    def execute(self, cursor, entities) -> None:
        execute_values(cursor, "INSERT INTO corpus_occurrences VALUES %s", entities, """
    (
        %(occurrence_id)s,
        %(guid)s,
        %(record_guid)s,
        %(modified_on)s,
        %(imaged)s,
        %(information_withheld)s,
        %(basis_of_record)s,
        %(herbarium)s,
        %(collection)s,
        %(dataset)s,
        %(accession)s,
        %(catalog)s,
        %(barcode)s,
        %(other_numbers)s,
        %(family)s,
        %(taxon_name)s,
        %(accepted)s,
        %(scientific_name)s,
        %(notho_genus)s,
        %(genus)s,
        %(notho_species)s,
        %(specific_epithet)s,
        %(specific_authors)s,
        %(infraspecific_rank)s,
        %(notho_infraspecies)s,
        %(infraspecific_epithet)s,
        %(infraspecific_authors)s,
        %(hybrid_symbol)s,
        %(notho_genus_2)s,
        %(genus_2)s,
        %(notho_species_2)s,
        %(specific_epithet_2)s,
        %(specific_authors_2)s,
        %(infraspecific_rank_2)s,
        %(notho_infraspecies_2)s,
        %(infraspecific_epithet_2)s,
        %(infraspecific_authors_2)s,
        %(cultivar)s,
        %(name_qualifier)s,
        %(qualifier_position)s,
        %(is_type)s,
        %(type_designation)s,
        %(site_number)s,
        %(collector)s,
        %(collector_number)s,
        %(other_collectors)s,
        %(day_collected)s,
        %(month_collected)s,
        %(year_collected)s,
        %(verbatim_collection_date)s,
        %(day_of_year)s,
        %(country)s,
        %(state_province)s,
        %(county)s,
        %(locality)s,
        %(site_description)s,
        %(verbatim_elevation)s,
        %(minimum_elevation_in_meters)s,
        %(maximum_elevation_in_meters)s,
        %(verbatim_depth)s,
        %(minimum_depth_in_meters)s,
        %(maximum_depth_in_meters)s,
        %(verbatim_coordinates)s,
        %(decimal_latitude)s,
        %(decimal_longitude)s,
        %(valid_lat_lng)s,
        %(geodetic_datum)s,
        %(coordinate_uncertainty_in_meters)s,
        %(georeferenced_by)s,
        %(georeference_sources)s,
        %(georeference_remarks)s,
        %(specimen_notes)s,
        %(phenology)s,
        %(cultivated)s,
        %(origin)s
      )
    """)

    @staticmethod
    def dictify(row) -> dict[str, any]:
        d = dict()

        # We should have exact columns
        if len(row) != 75:
            # Debugging
            # pp = pprint.PrettyPrinter(indent=2)
            # pp.pprint(d)
            # pp.pprint(row)
            raise ValueError(
                f"Error parsing row: expected row length 75 but recieved {len(row)}")

        d["occurrence_id"] = row.pop(0)
        d["guid"] = row.pop(0)
        d["record_guid"] = row.pop(0)
        d["modified_on"] = row.pop(0)
        d["imaged"] = row.pop(0)
        d["information_withheld"] = row.pop(0)
        d["basis_of_record"] = row.pop(0)
        d["herbarium"] = row.pop(0)
        d["collection"] = row.pop(0)
        d["dataset"] = row.pop(0)
        d["accession"] = row.pop(0)
        d["catalog"] = row.pop(0)
        d["barcode"] = row.pop(0)
        d["other_numbers"] = row.pop(0)
        d["family"] = row.pop(0)
        d["taxon_name"] = row.pop(0)
        d["accepted"] = row.pop(0)
        d["scientific_name"] = row.pop(0)
        d["notho_genus"] = row.pop(0)
        d["genus"] = row.pop(0)
        d["notho_species"] = row.pop(0)
        d["specific_epithet"] = row.pop(0)
        d["specific_authors"] = row.pop(0)
        d["infraspecific_rank"] = row.pop(0)
        d["notho_infraspecies"] = row.pop(0)
        d["infraspecific_epithet"] = row.pop(0)
        d["infraspecific_authors"] = row.pop(0)
        d["hybrid_symbol"] = row.pop(0)
        d["notho_genus_2"] = row.pop(0)
        d["genus_2"] = row.pop(0)
        d["notho_species_2"] = row.pop(0)
        d["specific_epithet_2"] = row.pop(0)
        d["specific_authors_2"] = row.pop(0)
        d["infraspecific_rank_2"] = row.pop(0)
        d["notho_infraspecies_2"] = row.pop(0)
        d["infraspecific_epithet_2"] = row.pop(0)
        d["infraspecific_authors_2"] = row.pop(0)
        d["cultivar"] = row.pop(0)
        d["name_qualifier"] = row.pop(0)
        d["qualifier_position"] = row.pop(0)
        d["is_type"] = row.pop(0)
        d["type_designation"] = row.pop(0)
        d["site_number"] = row.pop(0)
        d["collector"] = row.pop(0)
        d["collector_number"] = row.pop(0)
        d["other_collectors"] = row.pop(0)
        d["day_collected"] = row.pop(0)
        d["month_collected"] = row.pop(0)
        d["year_collected"] = row.pop(0)
        d["verbatim_collection_date"] = row.pop(0)
        d["day_of_year"] = row.pop(0)
        d["country"] = row.pop(0)
        d["state_province"] = row.pop(0)
        d["county"] = row.pop(0)
        d["locality"] = row.pop(0)
        d["site_description"] = row.pop(0)
        d["verbatim_elevation"] = row.pop(0)
        d["minimum_elevation_in_meters"] = row.pop(0)
        d["maximum_elevation_in_meters"] = row.pop(0)
        d["verbatim_depth"] = row.pop(0)
        d["minimum_depth_in_meters"] = row.pop(0)
        d["maximum_depth_in_meters"] = row.pop(0)
        d["verbatim_coordinates"] = row.pop(0)
        d["decimal_latitude"] = row.pop(0)
        d["decimal_longitude"] = row.pop(0)
        d["valid_lat_lng"] = row.pop(0)
        d["geodetic_datum"] = row.pop(0)
        d["coordinate_uncertainty_in_meters"] = row.pop(0)
        d["georeferenced_by"] = row.pop(0)
        d["georeference_sources"] = row.pop(0)
        d["georeference_remarks"] = row.pop(0)
        d["specimen_notes"] = row.pop(0)
        d["phenology"] = row.pop(0)
        d["cultivated"] = row.pop(0)
        d["origin"] = row.pop(0)

        TransformHelper.transform_invalid_booleans(
            d, ["imaged", "accepted", "is_type", "valid_lat_lng", "cultivated"])
        TransformHelper.transform_empty_to_none(d, d.keys())

        return d


class AnnotationsHandler(BaseHandler):
    def __init__(self, connection, valid_fkeys, path="corpus/annotations.txt"):
        super().__init__(connection, path, valid_fkeys, "corpus_annotations")

    def execute(self, cursor, entities) -> None:
        execute_values(cursor, "INSERT INTO corpus_annotations VALUES %s", entities, """
      (
        %(occurrence_id)s,
        %(current_annotation)s,
        %(sequence_number)s,
        %(family)s,
        %(scientific_name)s,
        %(notho_genus)s,
        %(genus)s,
        %(notho_species)s,
        %(specific_epithet)s,
        %(specific_authors)s,
        %(infraspecific_rank)s,
        %(notho_infraspecies)s,
        %(infraspecific_epithet)s,
        %(infraspecific_authors)s,
        %(hybrid_symbol)s,
        %(notho_genus_2)s,
        %(genus_2)s,
        %(notho_species_2)s,
        %(specific_epithet_2)s,
        %(specific_authors_2)s,
        %(infraspecific_rank_2)s,
        %(notho_infraspecies_2)s,
        %(infraspecific_epithet_2)s,
        %(infraspecific_authors_2)s,
        %(cultivar)s,
        %(name_qualifier)s,
        %(qualifier_position)s,
        %(nomenclatural_code)s,
        %(annotated_by)s,
        %(day_annotated)s,
        %(month_annotated)s,
        %(year_annotated)s,
        %(annotation_references)s,
        %(annotation_remarks)s
      )
    """)

    @staticmethod
    def dictify(row) -> dict[str, any]:
        d = dict()

        if len(row) != 34:
            raise ValueError(f"Expected 34 rows but received: {len(row)}")

        d["occurrence_id"] = row.pop(0)
        d["current_annotation"] = row.pop(0)

        # Sometimes there is not a Sequence Number in the annotations dataset
        seq_num = row.pop(0)
        if seq_num is not None and seq_num[0].isalpha():
            d['sequence_number'] = None
            d['family'] = seq_num
        else:
            d["sequence_number"] = seq_num
            d["family"] = row.pop(0)

        d["scientific_name"] = row.pop(0)
        d["notho_genus"] = row.pop(0)
        d["genus"] = row.pop(0)
        d["notho_species"] = row.pop(0)
        d["specific_epithet"] = row.pop(0)
        d["specific_authors"] = row.pop(0)
        d["infraspecific_rank"] = row.pop(0)
        d["notho_infraspecies"] = row.pop(0)
        d["infraspecific_epithet"] = row.pop(0)
        d["infraspecific_authors"] = row.pop(0)
        d["hybrid_symbol"] = row.pop(0)
        d["notho_genus_2"] = row.pop(0)
        d["genus_2"] = row.pop(0)
        d["notho_species_2"] = row.pop(0)
        d["specific_epithet_2"] = row.pop(0)
        d["specific_authors_2"] = row.pop(0)
        d["infraspecific_rank_2"] = row.pop(0)
        d["notho_infraspecies_2"] = row.pop(0)
        d["infraspecific_epithet_2"] = row.pop(0)
        d["infraspecific_authors_2"] = row.pop(0)
        d["cultivar"] = row.pop(0)
        d["name_qualifier"] = row.pop(0)
        d["qualifier_position"] = row.pop(0)
        d["nomenclatural_code"] = row.pop(0)
        d["annotated_by"] = row.pop(0)

        # Sometimes ICBN makes its way to these fields...
        day_annotated = row.pop(0)
        if day_annotated == 'ICBN':
            d['day_annotated'] = None
        else:
            d["day_annotated"] = day_annotated

        month_annotated = row.pop(0)
        if month_annotated == 'ICBN':
            d['month_annotated'] = None
        elif month_annotated != '' and month_annotated[0].isalpha():
            d['annotated_by'] = month_annotated
            d['month_annotated'] = None
        else:
            d["month_annotated"] = month_annotated

        # If 'ICBN' somehow makes its way here, then this field
        # is usually the annotated_by
        if month_annotated == 'ICBN':
            d["annotated_by"] = row.pop(0)
            d["year_annotated"] = None
        else:
            d["year_annotated"] = row.pop(0)

        d["annotation_references"] = row.pop(0)
        d["annotation_remarks"] = row.pop(0)

        TransformHelper.transform_invalid_booleans(d, ['current_annotation'])
        TransformHelper.transform_empty_to_none(d, d.keys())
        TransformHelper.transform_question_mark_to_none(d, ['sequence_number'])

        if d['sequence_number'] is not None and d['sequence_number'][0].isalpha():
            pp = pprint.PrettyPrinter()
            pp.pprint(d)

        return d


class TypesHandler(BaseHandler):
    def __init__(self, connection, valid_fkeys, path="corpus/types.txt"):
        super().__init__(connection, path, valid_fkeys, 'corpus_types')

    def execute(self, cursor, entities) -> None:
        logging.debug("Inserting %s entities", len(entities))
        execute_values(cursor, "INSERT INTO corpus_types VALUES %s", entities, """
      (
        %(occurrence_id)s,
        %(sequence_number)s,
        %(family)s,
        %(scientific_name)s,
        %(notho_genus)s,
        %(genus)s,
        %(notho_species)s,
        %(specific_epithet)s,
        %(specific_authors)s,
        %(infraspecific_rank)s,
        %(notho_infraspecies)s,
        %(infraspecific_epithet)s,
        %(infraspecific_authors)s,
        %(cultivar)s,
        %(type_designation)s,
        %(holotype_location)s,
        %(year_published)s,
        %(publication)s,
        %(notes)s
      )
    """)

    @staticmethod
    def dictify(row) -> dict[str, any]:
        d = dict()

        d["occurrence_id"] = int(row.pop(0))
        d["sequence_number"] = row.pop(0)
        d["family"] = row.pop(0)
        d["scientific_name"] = row.pop(0)
        d["notho_genus"] = row.pop(0)
        d["genus"] = row.pop(0)
        d["notho_species"] = row.pop(0)
        d["specific_epithet"] = row.pop(0)
        d["specific_authors"] = row.pop(0)
        d["infraspecific_rank"] = row.pop(0)
        d["notho_infraspecies"] = row.pop(0)
        d["infraspecific_epithet"] = row.pop(0)
        d["infraspecific_authors"] = row.pop(0)
        d["cultivar"] = row.pop(0)
        d["type_designation"] = row.pop(0)
        d["holotype_location"] = row.pop(0)
        d["year_published"] = row.pop(0)
        d["publication"] = row.pop(0)
        d["notes"] = row.pop(0)

        TransformHelper.transform_empty_to_none(d, d.keys())

        return d


class MediaHandler(BaseHandler):

    def __init__(self, connection, valid_fkeys, path="corpus/media.txt"):
        super().__init__(connection, path, valid_fkeys)

    def execute(self, cursor, entities):
        execute_values(cursor, "INSERT INTO corpus_media VALUES %s", entities, """
      (
        %(occurrence_id)s,
        %(modified_on)s,
        %(media_guid)s,
        %(file_name)s,
        %(file_format)s,
        %(viewer_format)s,
        %(thumbnail_url)s,
        %(file_url)s,
        %(viewer_url)s,
        %(date_created)s,
        %(created_by)s,
        %(publisher)s,
        %(license)s
      )
    """)

    @staticmethod
    def dictify(row) -> dict[str, any]:
        d = dict()

        d["occurrence_id"] = row.pop(0)
        d["modified_on"] = row.pop(0)
        d["media_guid"] = row.pop(0)
        d["file_name"] = row.pop(0)
        d["file_format"] = row.pop(0)
        d["viewer_format"] = row.pop(0)
        d["thumbnail_url"] = row.pop(0)
        d["file_url"] = row.pop(0)
        d["viewer_url"] = row.pop(0)
        d["date_created"] = row.pop(0)
        d["created_by"] = row.pop(0)
        d["publisher"] = row.pop(0)
        d["license"] = row.pop(0)

        TransformHelper.transform_empty_to_none(d, d.keys())

        return d


class Validator:
    def __init__(self):
        # A dictionary of values containing errors
        self.errors = {
            'corpus_occurrences': {},
            'corpus_annotations': {},
            'corpus_media': {},
            'corpus_types': {},
        }

        # A dictionary of sets to validate against
        self.sets = {
            'valid_occurrence_ids': set(),
        }

        # TODO:
        # Select all IDs from corpus_occurrences
        # add them to the set

    def _add_error_for_entity(self, table_name, entity_id, value: tuple[str, str]):
        # Try to get a key, empty list if not exists
        errors = self.errors[table_name].get(entity_id, [])
        # Add the value
        errors.append(value)
        self.errors[table_name][entity_id] = errors

    def validate_fkey(self, table_name, entity) -> list[any]:
        # Validates occurrence IDs

        valid_set = self.sets['valid_occurrence_ids']
        if entity['occurrence_id'] not in valid_set:
            error = ("Invalid foreign key", entity['occurrence_id'])
            self._add_error_for_entity(
                table_name, entity['occurrence_id'], error)
            return error

    def validate_unique(self, table_name, key, entity) -> tuple[str, str]:
        set_key = f'{table_name}:{key}'
        # Create a set if there isn't one already
        if set_key not in self.sets:
            self.sets[set_key] = set()

        if entity[key] is not None and entity[key] in self.sets[set_key]:
            error = (f'Duplicate {key} found', entity[key])
            self._add_error_for_entity(
                table_name,
                entity['occurrence_id'],
                error
            )
            return error

        self.sets[set_key].add(entity[key])
        return None

    def validate_length(self, table_name, key, entity, length=255) -> tuple[str, str]:
        if entity[key] is not None and len(entity[key]) > length:
            error = (
                f"Value exceeds maximum length ({length} chars) for key {key}", entity[key])
            self._add_error_for_entity(
                table_name, entity['occurrence_id'], error)
            return error
        return None

    def validate_integer(self, table_name, key, entity) -> tuple[str, str]:
        if entity[key] is None:
            return None
        try:
            int(entity[key])
            return None
        except ValueError:
            error = (f"Expected integer value for key {key}", entity[key])
            self._add_error_for_entity(
                table_name,
                entity['occurrence_id'],
                error
            )
            return error

    def validate_numeric(self, table_name, key, entity) -> tuple[str, str]:
        if entity[key] is None:
            return None

        try:
            Decimal(entity[key])
            return None
        except ValueError:
            error = (f"Expected decimal value for key {key}", entity[key])
            self._add_error_for_entity(
                table_name, entity['occurrence_id'], error)

    def validate_occurrence(self, entity):
        varchchar_255_fields = [
            "guid",
            "basis_of_record",
            "herbarium",
            "collection",
            "dataset",
            "accession",
            "catalog",
            "barcode",
            "other_numbers",
            "family",
            "taxon_name",
            "scientific_name",
            "notho_genus",
            "genus",
            "notho_species",
            "specific_epithet",
            "specific_authors",
            "infraspecific_rank",
            "notho_infraspecies",
            "infraspecific_epithet",
            "infraspecific_authors",
            "hybrid_symbol",
            "notho_genus_2",
            "genus_2",
            "notho_species_2",
            "specific_epithet_2",
            "specific_authors_2",
            "infraspecific_rank_2",
            "notho_infraspecies_2",
            "infraspecific_epithet_2",
            "infraspecific_authors_2",
            "cultivar",
            "name_qualifier",
            "qualifier_position",
            "type_designation",
            "site_number",
            "collector",
            "collector_number",
            "other_collectors",
            "day_collected",
            "month_collected",
            "year_collected",
            "verbatim_collection_date",
            "day_of_year",
            "country",
            "state_province",
            "county",
            "verbatim_elevation",
            "verbatim_depth",
            "verbatim_coordinates",
            "geodetic_datum",
            "georeferenced_by",
            "georeference_sources",
            "phenology",
            "origin"
        ]

        numeric_fields = [
            "minimum_elevation_in_meters",
            "maximum_elevation_in_meters",
            "minimum_depth_in_meters",
            "maximum_depth_in_meters",
            "decimal_latitude",
            "decimal_longitude",
            "coordinate_uncertainty_in_meters",
        ]

        e = self.validate_unique('corpus_occurrences', 'guid', entity)
        if e is None:
            self.sets['valid_occurrence_ids'].add(entity['occurrence_id'])

        for field in varchchar_255_fields:
            self.validate_length('corpus_occurrences', field, entity)
        for field in numeric_fields:
            self.validate_numeric('corpus_occurrences', field, entity)

    def validate_media(self, entity):
        varchar_255_fields = [
            "media_guid",
            "thumbnail_url",
            "file_url",
            "viewer_url",
            "date_created",
            "created_by",
            "publisher",
        ]

        varchar_124_fields = [
            "file_name",
            "file_format",
            "viewer_format",
        ]
        self.validate_fkey('corpus_media', entity)
        self.validate_length('corpus_media', 'license', entity, length=512)
        for field in varchar_255_fields:
            self.validate_length('corpus_media', field, entity)
        for field in varchar_124_fields:
            self.validate_length('corpus_media', field, entity)

    def validate_type(self, entity):
        varchar_255_fields = [
            "family",
            "scientific_name",
            "notho_genus",
            "genus",
            "notho_species",
            "specific_epithet",
            "specific_authors",
            "infraspecific_rank",
            "notho_infraspecies",
            "infraspecific_epithet",
            "infraspecific_authors",
            "cultivar",
            "type_designation",
            "holotype_location",
        ]

        integer_fields = ["year_published"]

        self.validate_fkey('corpus_types', entity)
        self.validate_length(
            'corpus_types', 'publication', entity, length=512)
        for field in varchar_255_fields:
            self.validate_length('corpus_types', field, entity)
        for field in integer_fields:
            self.validate_integer('corpus_types', field, entity)

    def validate_annotation(self, entity):
        varchar_255_fields = [
            "family",
            "scientific_name",
            "notho_genus",
            "genus",
            "notho_species",
            "specific_epithet",
            "specific_authors",
            "infraspecific_rank",
            "notho_infraspecies",
            "infraspecific_epithet",
            "infraspecific_authors",
            "hybrid_symbol",
            "notho_genus_2",
            "genus_2",
            "notho_species_2",
            "specific_epithet_2",
            "specific_authors_2",
            "infraspecific_rank_2",
            "notho_infraspecies_2",
            "infraspecific_epithet_2",
            "infraspecific_authors_2",
            "cultivar",
            "name_qualifier",
            "qualifier_position",
            "nomenclatural_code",
            "annotated_by",
            "annotation_references",
        ]

        integer_fields = [
            "sequence_number",
            "day_annotated",
            "month_annotated",
            "year_annotated",
        ]
        self.validate_fkey('corpus_annotations', entity)
        for field in varchar_255_fields:
            self.validate_length('corpus_annotations', field, entity)
        for field in integer_fields:
            self.validate_integer('corpus_annotations', field, entity)

    def _validate_occurrences(self, path='corpus/occurrences.txt'):
        with open(path, encoding='UTF-8') as file:
            # Configure our reader to use tab delimiters and no quotations
            rows = csv.reader(file, delimiter="\t", quoting=csv.QUOTE_NONE)
            num_rows = 0
            start = time.time()

            # Pop the header of our TSV
            next(rows)

            for row in rows:
                d = OccurencesHandler.dictify(row)
                self.validate_occurrence(d)
                num_rows += 1

            logging.info("Completed validation of %s rows in %fs for corpus_occurrences",
                         num_rows, time.time() - start)
            if len(self.errors['corpus_occurrences']) > 0:
                logging.warning(
                    "Found errors with %s corpus_occurrences entities.", len(self.errors['corpus_occurrences']))

    def _validate_media(self, path='corpus/media.txt'):
        with open(path, encoding='UTF-8') as file:
            # Configure our reader to use tab delimiters and no quotations
            rows = csv.reader(file, delimiter="\t", quoting=csv.QUOTE_NONE)
            num_rows = 0
            start = time.time()

            # Pop the header of our TSV
            next(rows)

            for row in rows:
                d = MediaHandler.dictify(row)
                self.validate_media(d)
                num_rows += 1

            logging.info("Completed validation of %s rows in %fs for corpus_media",
                         num_rows, time.time() - start)
            if len(self.errors['corpus_media']) > 0:
                logging.warning(
                    "Found errors with %s corpus_media entities.", len(self.errors['corpus_media']))

    def _validate_types(self, path='corpus/types.txt'):
        with open(path, encoding="UTF-8") as file:
            rows = csv.reader(file, delimiter='\t', quoting=csv.QUOTE_NONE)
            num_rows = 0
            start = time.time()

            # Pop the header of our TSV
            next(rows)

            for row in rows:
                d = TypesHandler.dictify(row)
                self.validate_type(d)
                num_rows += 1

            logging.info("Completed validation of %s rows in %fs for corpus_types",
                         num_rows, time.time() - start)
            if len(self.errors['corpus_types']) > 0:
                logging.warning("Found errors with %s corpus_types entities.", len(
                    self.errors['corpus_types']))

    def _validate_annotations(self, path='corpus/annotations.txt'):
        with open(path, encoding="UTF-8") as file:
            rows = csv.reader(file, delimiter='\t', quoting=csv.QUOTE_NONE)
            num_rows = 0
            start = time.time()

            # Pop the header of our TSV
            next(rows)

            for row in rows:
                d = AnnotationsHandler.dictify(row)
                self.validate_annotation(d)
                num_rows += 1

            logging.info("Completed validation of %s rows in %fs for corpus_annotations",
                         num_rows, time.time() - start)
            if len(self.errors['corpus_annotations']) > 0:
                logging.warning("Found errors with %s corpus_annotations entities", len(
                    self.errors['corpus_annotations']))

    def validate_corpus(self, occurrences_path="corpus/occurrences.txt", media_path="corpus/media.txt", types_path="corpus/types.txt", annotations_path="corpus/annotations.txt"):
        logging.info("Validating Occurrences...")
        self._validate_occurrences(occurrences_path)
        logging.info("Validating Media...")
        self._validate_media(media_path)
        logging.info("Validating Types...")
        self._validate_types(types_path)
        logging.info("Validating Annotations...")
        self._validate_annotations(annotations_path)
        # pp = pprint.PrettyPrinter()
        # pp.pprint(self.errors)

    def write(self, file_name='validation_errors.json'):
        with open(file_name, 'w', encoding='utf-8') as fout:
            pp = pprint.PrettyPrinter(stream=fout)
            pp.pprint(self.errors)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Migrate PNW Herbaria corpus to PostgreSQL')
    parser.add_argument(
        '-d', '--dry-run', help='perform database transactions but don\'t commit changes', action="store_true")
    parser.add_argument('-t', '--tables', nargs='+', help='specify which tables to migrate to Postgres',
                        choices=['occurrences', 'annotations', 'types', 'media'])
    parser.add_argument(
        '-n', '--no-validate', help='Ignore pre-validation of data', action="store_true")
    parser.add_argument(
        '-v', '--verbose', action="store_true"
    )
    args = parser.parse_args()

    dry_run = args.dry_run
    tables = args.tables
    no_validate = args.no_validate if args.no_validate else False
    verbose = args.verbose

    logging.basicConfig(level=logging.WARNING)

    if verbose:
      logging.set_level(level=logging.DEBUG)

    # Open up a DB connection
    pg_conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="devpass",
    )

    validator = Validator()
    if not no_validate:
        validator.validate_corpus()
        validator.write()
        logging.info("Validated %s ids", len(
            validator.sets['valid_occurrence_ids']))

    if dry_run:
        logging.warning('Dry run set to true.')

    if(tables is None or 'occurrences' in tables):
        logging.info("Inserting Occurrences...")
        occurrences_handler = OccurencesHandler(
            pg_conn, validator.sets['valid_occurrence_ids'])
        occurrences_handler.handle(
            skip_fkey_validation=no_validate, no_commit=dry_run)

    if(tables is None or 'annotations' in tables):
        logging.info("Inserting Annotations...")
        annotations_handler = AnnotationsHandler(
            pg_conn, validator.sets['valid_occurrence_ids'])
        annotations_handler.handle(
            skip_fkey_validation=no_validate, no_commit=dry_run)

    if(tables is None or 'types' in tables):
        logging.info("Inserting Types...")
        types_handler = TypesHandler(
            pg_conn, validator.sets['valid_occurrence_ids'])
        types_handler.handle(
            skip_fkey_validation=no_validate, no_commit=dry_run, batch_size=1)

    if(tables is None or 'media' in tables):
        logging.info("Inserting Media...")
        media_handler = MediaHandler(
            pg_conn, validator.sets['valid_occurrence_ids'])
        media_handler.handle(
            skip_fkey_validation=no_validate, no_commit=dry_run)

    pg_conn.close()

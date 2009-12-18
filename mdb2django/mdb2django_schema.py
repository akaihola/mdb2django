#!/usr/bin/env python

"""
Extract the database schema from a Microsoft Access 2000 .MDB file and
convert it to Django ``models.py`` and ``admin.py`` files.

Requirements:
 * A Java Virtual Machine, tested on `OpenJDK`_ 6
 * A Python environment capable of accessing Java libraries, e.g.:

   - `Jython`_ 2.5
   - `CPython`_ 2.5 and `JPype`_ 0.5

 * `Jackcess`_ 1.1.20
 * Apache `Commons Logging`_
 * Apache `Commons Lang`_

.. _OpenJDK: http://openjdk.java.net/
.. _Jython: http://jython.org/
.. _CPython: http://python.org/
.. _JPype: http://jpype.sourceforge.net/
.. _Jackcess: http://jackcess.sourceforge.net/
.. _Commons Logging: http://commons.apache.org/logging/
.. _Commons Lang: http://commons.apache.org/lang/
"""

import re
import sys
import json
import itertools
from collections import defaultdict

MEMO_LENGTH = 8190

def memoize(method):
    def wrapped(self):
        if self not in wrapped.cache:
            wrapped.cache[self] = method(self)
        return wrapped.cache[self]
    wrapped.cache = {}
    return wrapped

memoized_property = lambda method: property(memoize(method))

MONTH_ABBRS = 'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()

class ValueConversion:
    def __init__(self, custom_conversion=lambda t, c, v: v):
        self.custom_conversion = custom_conversion

    def java2python(self, table_name, column_name, value):
        """Convert a Java column value to a Python value

        Booleans and timestamps need special conversion.
        """
        cls = value.__class__.__name__
        if cls in ('java.lang.Integer', 'java.lang.Short'):
            value = value.value
        elif cls == 'unicode':
            value = value.replace('\r\n', r'\r').replace('\t', r'\t')
        elif cls == 'java.lang.Boolean':
            value = bool(value.booleanValue())
        elif cls in ('com.healthmarketscience.jackcess.Column$DateExt',
                   'java.util.Date' # for unit tests
                   ):
            _, m_abbr, d, HMS, _, Y = value.toString().split()
            value = '%s-%02d-%s %s' % (
                Y, MONTH_ABBRS.index(m_abbr) + 1, d, HMS)
        return self.custom_conversion(table_name, column_name, value)

    def java2json(self, table_name, column_name, value):
        """Convert a Java column value to a JSON value

        Booleans, nulls and timestamps need special conversion.
        """
        python_value = self.java2python(table_name, column_name, value)
        if python_value is None:
            return 'null'
        return python_value

    def java2pgcopy(self, table_name, column_name, value):
        """Format a Java column value as a PostgreSQL COPY command column value

        Convert to UTF-8 representations, except booleans to t/f.
        """
        python_value = self.java2python(table_name, column_name, value)
        if isinstance(python_value, bool):
            return 'ft'[python_value]
        if python_value is None:
            return r'\N'
        return unicode(python_value).encode('UTF-8')

def forloop(seq):
    """Iterate sequence with markers for first and last item

    Yields a 3-tuple (is_first, item, is_last) for each item in the
    sequence.
    """
    first = True
    for item in seq:
        if not first:
            yield last_was_first, last_item, False
        last_was_first, first, last_item = first, False, item
    if 'last_item' in locals():
        yield last_was_first, last_item, True

CAMELCASE2EN_RE = re.compile(r'([a-z])([A-Z])')

def camelcase2english(s):
    return ' '.join('%s%s' % (part[0].upper(), part[1:])
                    for part in (CAMELCASE2EN_RE.sub(r'\1 \2', s)
                                 .replace('_', ' ')
                                 .split(' ')))

def underscores2camelcase(s):
    return ''.join(part.title() for part in s.split('_'))

class Relationship:
    def __init__(self, database, access_relationship):
        self.database = database
        to_model = database.get_model_by_table(access_relationship.toTable)
        from_model = database.get_model_by_table(access_relationship.fromTable)
        self.to_field = to_model.get_field_by_column(
            access_relationship.toColumns[0])
        self.from_field = from_model.get_field_by_column(
            access_relationship.fromColumns[0])

    def __repr__(self):
        return '<Relationship from:%s to:%s>' % (self.from_field, self.to_field)

class FieldBase:
    def as_python(self):
        yield '    %s = models.%s(' % (self.name, self.field_class)
        for first, att, last in forloop(self.attrs):
            yield '        %s%s' % (att, ',)'[last])

class Field(FieldBase):
    def __init__(self, model, column):
        self.model = model
        self.database = model.database
        self.column = column

    @property
    def name(self):
        return self.database.column2field_name(
            self.column.name, self.primary_key)

    @property
    def verbose_name(self):
        return camelcase2english(self.name)

    @property
    def foreign_key(self):
        if self not in self.database.relationships:
            return False
        return self.database.relationships[self]

    @property
    def reverse_foreign_keys(self):
        for r in self.database.reverse_relationships[self]:
            yield r

    @property
    def field_class(self):
        if self.foreign_key:
            return 'ForeignKey'
        if self.column.type.name() == u'TEXT':
            if self.column.length == MEMO_LENGTH:
                return 'TextField'
            else:
                return 'CharField'
        elif self.column.type.name() in (u'INT', u'LONG'):
            if self.primary_key:
                return 'AutoField'
            return 'IntegerField'
        elif self.column.type.name() == u'BOOLEAN':
            return 'BooleanField'
        elif self.column.type.name() == u'SHORT_DATE_TIME':
            return 'DateTimeField'

    @property
    def inline_class_name(self):
        if len(self.model.foreign_key_fields) > 1:
            return '%s%sInline' % (self.model.name,
                                   underscores2camelcase(self.name))
        return '%sInline' % self.model.name

    @property
    def index(self):
        return self.model.single_column_indexes[self.column.name]

    @property
    def primary_key(self):
        try:
            return self.index.isPrimaryKey()
        except KeyError:
            return False

    @property
    def attrs(self):
        """
        Generate attributes for the Django model field as (str, bool)
        tuples.  The first item is the definition of the attribute as
        Python code, e.g. 'max_length=5', and the second item is True
        only for the last attribute.
        """
        if self.foreign_key: # ForeignKey(model, to_field=, verbose_name=)
            relation = self.foreign_key
            yield relation.from_field.model.name
            if relation.from_field.name != 'id':
                yield "to_field='%s'" % relation.from_field.name
            yield "verbose_name=_(u'%s')" % self.verbose_name
        else:
            yield "_(u'%s')" % self.verbose_name
            if self.column.type.name() == u'TEXT':
                if self.column.length != MEMO_LENGTH:
                    yield 'max_length=%d' % self.column.length
        try:
            if self.primary_key:
                yield 'primary_key=True'
            elif self.index:
                yield 'db_index=True'
                if self.index.isUnique():
                    yield 'unique=True'
        except KeyError:
            pass

        if self.name != self.column.name:
            yield "db_column='%s'" % self.column.name

    def __repr__(self):
        return '<Field %s.%s>' % (self.model.name, self.name)

class PrimaryKeyField:
    """A generated primary key field for tables lacking one

    This class contains just enough functionality to act as a hidden
    Django AutoField primary key.  It is used when an Access table
    lacks a single field primary key.
    """
    name = 'id'
    primary_key = True
    class column:
        name = None
    foreign_key = False
    reverse_foreign_keys = ()
    def __init__(self, model):
        pass
    def as_python(self):
        return ()

class Model:
    def __init__(self, database, access_table):
        self.database = database
        self.access_table = access_table

    @memoized_property
    def single_column_indexes(self):
        return dict(
            (i.columns[0].name, i) for i in self.access_table.indexes
            if len(i.columns) == 1)

    @property
    def multicolumn_indexes(self):
        return [i for i in self.access_table.indexes if len(i.columns) > 1]

    @property
    def foreign_keys(self):
        for field in self.fields:
            if field.foreign_key:
                yield field.foreign_key

    @property
    def related_models(self):
        return frozenset(relation.from_field.model
                         for relation in self.foreign_keys)

    @property
    def foreign_key_fields(self):
        return frozenset(relation.to_field for relation in self.foreign_keys)

    @property
    def reverse_foreign_keys(self):
        for field in self.fields:
            for relation in field.reverse_foreign_keys:
                yield relation

    @property
    def name(self):
        return self.database.table2model_name(self.access_table.name)

    @property
    def verbose_name(self):
        return camelcase2english(self.name)

    @property
    def verbose_name_plural(self):
        return '%ss' % camelcase2english(self.name)

    @memoized_property
    def fields(self):
        _field_list = sorted((Field(self, c)
                              for c in self.access_table.getColumns()),
                             key=lambda f: not f.primary_key)
        if not _field_list[0].primary_key:
            # no primary key in access table, add an AutoField
            _field_list.insert(0, PrimaryKeyField(self))
        return _field_list

    @memoized_property
    def fields_by_column_name(self):
        return dict((field.column.name, field)
                    for field in self.fields)

    def get_field_by_column(self, column):
        try:
            return self.fields_by_column_name[column.name]
        except KeyError:
            raise KeyError('No column name "%s" in table "%s"; fields = %r' % (
                    column.name,
                    self.name,
                    self.fields_by_column_name))

    @property
    def primary_key(self):
        for field in self.fields:
            if field.primary_key:
                return field
        raise ValueError('No primary key for %r' % self)

    @property
    def db_table(self, app_name='myapp'):
        if self.database.keep_table_names:
            return self.access_table.name
        else:
            return '%s_%s' % (app_name, self.name.lower())

    def as_python(self):
        yield ''
        yield 'class %s(models.Model):' % self.name
        for field in self.fields:
            for line in field.as_python():
                yield line
        yield ''
        yield '    class Meta:'
        if self.database.keep_table_names or self.database.schema:
            db_table = self.db_table
            if self.database.schema:
                db_table = r"%s\".\"%s" % (self.database.schema, db_table)
            yield "        db_table = '%s'" % db_table
        if self.multicolumn_indexes:
            yield '        unique_together = ('
            for index in self.multicolumn_indexes:
                yield '            (%s),' % (
                    ' '.join("'%s'," % self.get_field_by_column(c).name
                             for c in index.columns))
            yield '        )'
        yield "        verbose_name = _(u'%s')" % self.verbose_name
        yield "        verbose_name_plural = _(u'%s')" % (
            self.verbose_name_plural)

    def inlines_as_python(self):
        fields = self.foreign_key_fields
        for field in fields:
            yield ''
            yield 'class %s(admin.TabularInline):' % (
                field.inline_class_name)
            yield '    model = %s' % self.name
            if len(fields) > 1:
                yield "    fk_name = '%s'" % field.name

    @property
    def inline_class_names(self):
        return (fk.to_field.inline_class_name
                for fk in self.reverse_foreign_keys)

    def output_admin(self):
        yield ''
        yield 'admin.site.register('
        yield '    %s,' % self.name
        inlines = list(forloop(self.inline_class_names))
        yield '    list_display=(%s)%s' % (
            ', '.join("'%s'" % f.name for f in self.fields),
            ',' if inlines else ')')
        for first, inline_name, last in inlines:
            if first and last:
                yield '    inlines=[%s])' % inline_name
            else:
                if first:
                    yield '    inlines=['
                if not last:
                    yield '        %s,' % inline_name
                else:
                    yield '        %s])' % inline_name

    def get_rows(self):
        self.access_table.reset()
        row_generator = (self.access_table.getNextRow()
                         for i in itertools.repeat(None))
        return itertools.takewhile(lambda row: row is not None, row_generator)

    @property
    def row_count(self):
        return self.access_table.getRowCount()

    def output_fixture(self, app_name, valueconversion):
        "Output all rows from the model table as JSON"
        # helper function for converting Jackcess column values to
        # JSON:
        fix_value = lambda field_name, value: valueconversion.java2json(
            self.access_table.name, field_name, value)
        try: # Access table has a single-field primary key
            pk_index = self.primary_key.column.columnIndex
            get_pk = lambda values_list: fix_value(
                self.primary_key.column.name, values_list[pk_index])
        except AttributeError: # generate an AutoField
            counter = itertools.count()
            get_pk = lambda row: counter.next()
        for row_is_first, row, row_is_last in forloop(self.get_rows()):
            values_list = list(row.values())
            data = dict(
                pk=get_pk(values_list),
                model='%s.%s' % (app_name, self.name.lower()),
                fields=dict((field_name, fix_value(field_name, value))
                            for field_name, value in
                            zip(row.keySet(), values_list)
                            if field_name != self.primary_key.name))
            json_lines = json.dumps(data).split('\n')
            for line_is_first, line, line_is_last in forloop(json_lines):
                if row_is_last and line_is_last:
                    yield line
                else:
                    yield line + ','

    @memoized_property
    def pg_table(self):
        db_table = '"%s"' % self.db_table
        if self.database.schema:
            db_table = '"%s".%s' % (self.database.schema, db_table)
        return db_table

    def delete_as_pg(self):
        return 'DELETE FROM %s;' % self.pg_table

    def output_postgresql(self, valueconversion):
        "Output all rows from the table as PostgreSQL COPY commands"
        # get fields in MDB order, exclude added AutoFields
        column_names = [column.name
                        for column in self.access_table.getColumns()]
        yield 'COPY %s (%s) FROM stdin;' % (
            self.pg_table, ', '.join('"%s"' % n for n in column_names))
        for row in self.get_rows():
            yield '\t'.join(
                valueconversion.java2pgcopy(self.access_table.name,
                                            column_names[index],
                                            value)
                for index, value in enumerate(row.values().toArray()))
        yield r'\.'
        yield ''

    def __repr__(self):
        return '<Model %s>' % self.name

class OutputType(object):
    def __init__(self, name, title, comment_char, work):
        self.name = name
        self.title = title
        self.comment_char = comment_char
        self.work = work

    @property
    def attr(self):
        return '%s_file' % self.name

    @property
    def long(self):
        return '--%s-file' % self.name

    @property
    def method_name(self):
        return 'output_%s' % self.name

class DatabaseWrapper:

    OUTPUT_TYPES = [
        OutputType('models', 'models.py', '#', 5.0),
        OutputType('admin', 'admin.py', '#', 1.0),
        OutputType('fixture', 'fixture.json', '#', 150.0),
        OutputType('postgresql', 'pg_data.sql', '-', 40.0)]

    def __init__(self, db,
                 app_name='myapp',
                 schema=None,
                 keep_table_names=False,
                 table2model_name=lambda s: s,
                 column2field_name=lambda c, pk: c,
                 custom_conversion=lambda t, c, v: v):
        self.db = db
        self.app_name = app_name
        self.schema = schema
        self.keep_table_names = keep_table_names
        self.table2model_name = table2model_name
        self.column2field_name = column2field_name
        self.valueconversion = ValueConversion(custom_conversion)

    @classmethod
    def from_file(cls, filepath, **kwargs):
        try: # jython
            from com.healthmarketscience.jackcess import Database
            from java.io import File
        except ImportError: # JPype
            from jpype import startJVM, getDefaultJVMPath, JPackage, java
            startJVM(getDefaultJVMPath())
            com = JPackage('com')
            Database = com.healthmarketscience.jackcess.Database
            File = java.io.File
        return cls(Database.open(File(filepath), True), # True = read-only
                   **kwargs)

    def _add_relationships(self, result, table_names=None):
        a = result['all']
        if table_names and len(table_names) > 1:
            for table_name in table_names[1:]:
                access_relationships = self.db.getRelationships(
                    self.db.getTable(table_names[0]),
                    self.db.getTable(table_name))
                for access_relationship in access_relationships:
                    r = Relationship(self, access_relationship)
                    a[r.to_field, r.from_field] = r
            self._add_relationships(result, table_names[1:])

    @memoize
    def get_relationships(self):
        all_ = {}
        forward = {}
        reverse = defaultdict(set)
        relationships = {
            'all': all_,
            'forward': forward,
            'reverse': reverse}
        self._add_relationships(relationships, list(self.db.getTableNames()))
        for (to_field, from_field), relationship in all_.iteritems():
            forward[to_field] = relationship
            reverse[from_field].add(relationship)
        return relationships

    @property
    def relationships(self):
        return self.get_relationships()['forward']

    @property
    def reverse_relationships(self):
        return self.get_relationships()['reverse']

    def get_model_by_table(self, access_table):
        if not hasattr(self, '_models_by_table_name'):
            self._models_by_table_name = dict(
                (model.access_table.name, model) for model in self.models)
        return self._models_by_table_name[access_table.name]

    def order_models(self, models, done=set()):
        """Sorts models based on foreign key dependencies

        Related models appear before the corresponding tables with the
        foreign key.
        """
        for model in models:
            if model.name is None:
                continue
            if model in done:
                continue
            done.add(model)
            for related_model in self.order_models(model.related_models, done):
                yield related_model
            yield model

    @memoized_property
    def models(self):
        """
        If a table name to model name translation function is used
        (see the `table2model_name` constructor attribute), it can
        prevent tables from being processed by returning None instead
        of a valid table name.
        """
        all_models = [Model(self, self.db.getTable(table_name))
                      for table_name in self.db.getTableNames()]
        return [m for m in all_models if m.name is not None]

    @memoized_property
    def ordered_models(self):
        return list(self.order_models(self.models))

    def output_models(self):
        yield len(self.models) + 2, 'generating model imports'
        yield 'from django.db import models'
        yield 'from django.utils.translation import ugettext as _'
        for index, model in enumerate(self.ordered_models):
            yield len(self.models) - index, 'generating models: %s' % model.name
            for line in model.as_python():
                yield line

    def output_admin(self):
        yield 2 * len(self.models) + 2, 'generating admin imports'
        yield 'from django.contrib import admin'
        yield 'from %s.models import (' % self.app_name
        for index, model in enumerate(self.ordered_models):
            yield 2 * len(self.models) + 1, 'generating admin model imports'
            yield '    %s,' % model.name
        yield ')'
        for index, model in enumerate(self.ordered_models):
            yield (2 * len(self.models) - index,
                   'generating admin inline: %s', model.name)
            for line in model.inlines_as_python():
                yield line
        for index, model in enumerate(self.ordered_models):
            yield (len(self.models) - index,
                   'generating ModelAdmin: %s' % model.name)
            for line in model.output_admin():
                yield line

    def total_data_lines(self):
        return sum((model.row_count for model in self.models), 0)

    def output_fixture(self):
        "Output all data from the database as a JSON fixture"
        total = self.total_data_lines()
        for model_is_first, model, model_is_last in forloop(self.models):
            lines = forloop(model.output_fixture(
                    self.app_name, self.valueconversion))
            for line_is_first, line, line_is_last in lines:
                yield total, 'generating JSON fixture: %s' % model.name
                total -= 1
                yield (' ['[model_is_first and line_is_first] +
                       line +
                       ('', ']')[model_is_last and line_is_last])

    def output_postgresql(self):
        "Output all data from the database as PostgreSQL COPY commands"
        counter = len(self.models) + self.total_data_lines()
        for model in reversed(self.ordered_models):
            yield counter, 'generating SQL DELETE clauses: %s' % model.name
            counter -= 1
            yield model.delete_as_pg()
        for model in self.ordered_models:
            for line in model.output_postgresql(self.valueconversion):
                yield counter, 'generating SQL COPY lines: %s' % model.name
                counter -= 1
                yield line

    def __repr__(self):
        return '<Database %d>' % id(self.db)

def make_option_parser():
    from optparse import OptionParser
    p = OptionParser()
    for output_type in DatabaseWrapper.OUTPUT_TYPES:
        p.add_option('-%s' % output_type.name[0],
                     output_type.long,
                     action='store')
    p.add_option('-n', '--app-name', action='store', default='myapp')
    p.add_option('-s', '--schema', action='store')
    p.add_option('-k', '--keep-table-names', action='store_true')
    p.add_option('-P', '--progress', action='store_true')
    p.add_option('-d', '--debug', action='store')
    return p

def check_arguments(option_parser, opts, args):
    if len(args) != 1:
        option_parser.error('only one argument expected')

def make_database_wrapper(opts, args,
                          table2model_name=lambda s: s,
                          column2field_name=lambda c, pk: c,
                          custom_conversion=lambda t, c, v: v):
    return DatabaseWrapper.from_file(args[0],
                                     app_name=opts.app_name,
                                     schema=opts.schema,
                                     keep_table_names=opts.keep_table_names,
                                     table2model_name=table2model_name,
                                     column2field_name=column2field_name,
                                     custom_conversion=custom_conversion)

def write_to_file_or_stdout(line_generator, filepath, title, progress_callback,
                            comment_char='#'):
    if filepath is None:
        return None
    if filepath == '-':
        output = sys.stdout
        output.write('\n\n%s %s %s\n\n' % ((68-len(title)) * comment_char,
                                           title,
                                           2*comment_char))
    else:
        output = file(filepath, 'w')
    lines = line_generator()
    total_estimate = None
    for item in lines:
        if isinstance(item, (str, unicode)):
            print >>output, item
        elif progress_callback:
            # `item` is tuple (number of lines remaining, message)
            if total_estimate is None:
                total_estimate = float(item[0])
            progress_callback(1.0 - (item[0] / total_estimate), item[1])

def run_conversion(dbwrapper, opts):
    total_work = sum((t.work for t in dbwrapper.OUTPUT_TYPES
                      if getattr(opts, t.attr) is not None),
                     0.0)
    work_offset = 0.0
    for output_type in dbwrapper.OUTPUT_TYPES:
        filepath = getattr(opts, output_type.attr)
        if filepath is None:
            continue

        if opts.progress:
            def progress_callback(progress, message):
                progress = max(0.0, min(100.0, progress))
                current = int(100.0 *
                              (work_offset + progress * output_type.work) /
                              total_work)
                if (current, message) != progress_callback.previous:
                    print current, message
                    progress_callback.previous = current, message
            progress_callback.previous = 0, ''
        else:
            progress_callback = None

        write_to_file_or_stdout(getattr(dbwrapper, output_type.method_name),
                                filepath,
                                output_type.title,
                                progress_callback,
                                comment_char=output_type.comment_char)
        work_offset += output_type.work

    if opts.debug: # print list of relations as Python comments
        for (to_table, to_column), relation in d.relationships.items():
            print '# %s.%s -> %s.%s' % (
                to_table, to_column,
                relation.fromTable.name, relation.fromColumns[0].name)

if __name__ == '__main__':
    p = make_option_parser()
    opts, args = p.parse_args()
    check_arguments(p, opts, args)
    d = make_database_wrapper(opts, args)
    run_conversion(d, opts)

#!/usr/bin/env python

"""
Extract the database schema from a Microsoft Access 2000 .MDB file and
convert it to Django ``models.py`` and ``admin.py`` files.

Requirements:
 * A Python environment capable of accessing Java libraries, e.g.:

   - `Jython`_ 2.5
   - `CPython`_ 2.5 and `JPype`_ 0.5

 * `Jackcess`_ 1.1.20
 * Apache `Commons Logging`_
 * Apache `Commons Lang`_

.. _Jython: http://jython.org/
.. _CPython: http://python.org/
.. _JPype: http://jpype.sourceforge.net/
.. _Jackcess: http://jackcess.sourceforge.net/
.. _Commons Logging: http://commons.apache.org/logging/
.. _Commons Lang: http://commons.apache.org/lang/
"""

import re
import sys
from collections import defaultdict

MEMO_LENGTH = 8190

def forloop(seq):
    items = list(seq)
    if items:
        for index, item in enumerate(items):
            yield index == 0, item, index == len(items)-1

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
        self.to_field = to_model.get_field_for_column(
            access_relationship.toColumns[0])
        self.from_field = from_model.get_field_for_column(
            access_relationship.fromColumns[0])

    def __repr__(self):
        return '<Relationship from:%s to:%s>' % (self.from_field, self.to_field)

class Field:
    def __init__(self, model, column):
        self.model = model
        self.database = model.database
        self.column = column

    def as_python(self):
        yield '    %s = %s(' % (self.name, self.field_class)
        for first, att, last in forloop(self.attrs):
            yield '        %s%s' % (att, ',)'[last])

    @property
    def name(self):
        return self.database.column2field_name(
            self.column.name, self.primary_key)

    @property
    def verbose_name(self):
        return self.name.replace('_', ' ').title()

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
        except (AttributeError, KeyError): # jython: KeyError only
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
                yield 'index=True'
                if self.index.isUnique():
                    yield 'unique=True'
        except KeyError:
            pass

        if self.name != self.column.name:
            yield "db_column='%s'" % self.column.name

    def __repr__(self):
        return '<Field %s.%s>' % (self.model.name, self.name)

class Model:
    def __init__(self, database, access_table):
        self.database = database
        self.access_table = access_table

    @property
    def single_column_indexes(self):
        if not hasattr(self, '_single_column_indexes'):
            self._single_column_indexes = dict(
                (i.columns[0].name, i) for i in self.access_table.indexes
                if len(i.columns) == 1)
        return self._single_column_indexes

    @property
    def multicolumn_indexes(self):
        return [i for i in self.access_table.indexes if len(i.columns) > 1]

    @property
    def foreign_keys(self):
        for field in self.fields:
            if field.foreign_key:
                yield field.foreign_key

    @property
    def foreign_key_fields(self):
        return frozenset(relation.to_field
                         for relation in self.foreign_keys)

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

    @property
    def fields(self):
        if not hasattr(self, '_field_list'):
            self._field_list = sorted((Field(self, c)
                                       for c in self.access_table.getColumns()),
                                      key=lambda f: not f.primary_key)
        return self._field_list

    def get_field_for_column(self, column):
        if not hasattr(self, '_fields_by_column'):
            self._fields_by_column = dict((field.column.name, field)
                                          for field in self.fields)
        return self._fields_by_column[column.name]

    def as_python(self):
        yield ''
        yield 'class %s(models.Model):' % self.name
        for field in self.fields:
            for line in field.as_python():
                yield line
        yield ''
        yield '    class Meta:'
        if self.database.keep_table_names or self.database.schema:
            db_table = self.access_table.name
            if self.database.schema:
                db_table = '"%s"."%s"' % (self.database.schema, db_table)
            yield "        db_table = '%s'" % db_table
        if self.multicolumn_indexes:
            yield '        unique_together = ('
            for index in self.multicolumn_indexes:
                yield '            (%s),' % (
                    ' '.join("'%s'," % c.name for c in index.columns))
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

    def admin_as_python(self):
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

    def __repr__(self):
        return '<Model %s>' % self.name

class DatabaseWrapper:
    def __init__(self, db,
                 schema=None,
                 keep_table_names=False,
                 table2model_name=lambda s: s,
                 column2field_name=lambda c, pk: c):
        self.db = db
        self.schema = schema
        self.keep_table_names = keep_table_names
        self.table2model_name = table2model_name
        self.column2field_name = column2field_name

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

    def _add_relationships(self, table_names=None):
        a = self._relationships['all']
        if table_names and len(table_names) > 1:
            for table_name in table_names[1:]:
                relationships = self.db.getRelationships(
                    self.db.getTable(table_names[0]),
                    self.db.getTable(table_name))
                for access_relationship in relationships:
                    r = Relationship(self, access_relationship)
                    a[r.to_field, r.from_field] = r
            self._add_relationships(table_names[1:])

    def get_relationships(self):
        if not hasattr(self, '_relationships'):
            all_ = {}
            forward = {}
            reverse = defaultdict(set)
            self._relationships = {
                'all': all_,
                'forward': forward,
                'reverse': reverse}
            self._add_relationships(list(self.db.getTableNames()))
            for (to_field, from_field), relationship in all_.iteritems():
                forward[to_field] = relationship
                reverse[from_field].add(relationship)
        return self._relationships

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

    @property
    def models(self):
        """
        If a table name to model name translation function is used
        (see the `table2model_name` constructor attribute), it can
        prevent tables from being processed by returning None instead
        of a valid table name.
        """
        if not hasattr(self, '_model_list'):
            all_models = [Model(self, self.db.getTable(table_name))
                          for table_name in self.db.getTableNames()]
            self._model_list = [m for m in all_models if m.name is not None]
        return self._model_list

    def models_as_python(self):
        yield 'from django.db import models'
        for model in self.models:
            if model.name is None:
                continue
            for line in model.as_python():
                yield line

    def admin_as_python(self):
        yield 'from django.contrib import admin'
        yield 'from myapp.models import ('
        for model in self.models:
            yield '    %s,' % model.name
        yield ')'
        for model in self.models:
            for line in model.inlines_as_python():
                yield line
        for model in self.models:
            for line in model.admin_as_python():
                yield line

    def __repr__(self):
        return '<Database %d>' % id(self.db)

def make_option_parser():
    from optparse import OptionParser
    p = OptionParser()
    p.add_option('-m', '--models-file', action='store')
    p.add_option('-a', '--admin-file', action='store')
    p.add_option('-s', '--schema', action='store')
    p.add_option('-k', '--keep-table-names', action='store_true')
    p.add_option('-d', '--debug', action='store')
    return p

def check_arguments(opts, args):
    if len(args) != 1:
        option_parser.error('only one argument expected')

def make_database_wrapper(opts, args,
                          table2model_name=lambda s: s,
                          column2field_name=lambda c, pk: c):
    return DatabaseWrapper.from_file(args[0],
                                     schema=opts.schema,
                                     keep_table_names=opts.keep_table_names,
                                     table2model_name=table2model_name,
                                     column2field_name=column2field_name)

def file_or_stdout(filepath, title):
    if filepath:
        return file(filepath, 'w')
    sys.stdout.write('\n\n%s %s ##\n\n' % (
            (68-len(title)) * '#', title))
    return sys.stdout

def run_conversion(dbwrapper, opts):
    output = file_or_stdout(opts.models_file, 'models.py')
    for line in dbwrapper.models_as_python():
        print >>output, line

    output = file_or_stdout(opts.admin_file, 'admin.py')
    for line in dbwrapper.admin_as_python():
        print >>output, line

    if opts.debug: # print list of relations as Python comments
        for (to_table, to_column), relation in d.relationships.items():
            print '# %s.%s -> %s.%s' % (
                to_table, to_column,
                relation.fromTable.name, relation.fromColumns[0].name)

if __name__ == '__main__':
    p = make_option_parser()
    opts, args = p.parse_args()
    check_arguments(opts, args)
    d = make_database_wrapper(opts, args)
    run_conversion(d, opts)

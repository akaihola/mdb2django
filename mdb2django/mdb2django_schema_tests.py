from nose.tools import eq_, assert_true, assert_false, assert_raises

from mdb2django_schema import (
    java2python,
    forloop,
    camelcase2english,
    Relationship,
    DatabaseWrapper,
    Model,
    Field)

try: # jython
    import java
except ImportError: # JPype
    from jpype import startJVM, getDefaultJVMPath, JPackage, JClass, java
    startJVM(getDefaultJVMPath())
    com = JPackage('com')

class Java2Python_Tests:
    def setUp(self):
        try: # jython
            from java.lang import Integer, Short, Boolean
            from java.util import Date
        except ImportError: # JPype
            global Integer, Short, Boolean, Date
            Integer = java.lang.Integer
            Short = java.lang.Short
            Boolean = java.lang.Boolean
            Date = java.util.Date

    def test_integer(self):
        eq_(java2python(Integer(23)), 23)

    def test_short(self):
        eq_(java2python(Short(23)), 23)

    def test_boolean_false(self):
        eq_(java2python(Boolean(False)), False)

    def test_boolean_True(self):
        eq_(java2python(Boolean(True)), True)

    def test_date(self):
        eq_(java2python(Date(0, 0, 0, 0, 50, 0)), u'1899-12-31 00:50:00')
        eq_(java2python(Date(109, 11, 10, 13, 59, 15)), u'2009-12-10 13:59:15')

class ForLoop_Tests:
    def test_empty_sequence(self):
        "An empty sequence should yield no results"
        eq_(list(forloop([])), [])

    def test_one_item_sequence(self):
        "The only item of a one-item sequence is both first and last"
        items = forloop(['the only item'])
        first, item, last = items.next()
        assert_true(first)
        eq_(item, 'the only item')
        assert_true(last)
        assert_raises(StopIteration, items.next)

    def test_two_item_sequence(self):
        "Item 1 of a two-item sequence is the first and item 2 the last"
        eq_(list(forloop(['the first item', 'the second item'])),
            [(True, 'the first item', False),
             (False, 'the second item', True)])

    def test_multiple_item_sequence(self):
        "Middle items of a multi-item sequence are neither first or last"
        eq_(list(forloop([1, 2, 3, 4])),
            [(True, 1, False),
             (False, 2, False),
             (False, 3, False),
             (False, 4, True)])

class CamelCaseToEnglish_Tests:
    def test_insert_space_before_capital(self):
        eq_(camelcase2english('aB'), 'A B')

    def test_does_not_insert_space_between_capitals(self):
        eq_(camelcase2english('ID'), 'ID')

    def test_separates_multiple_words(self):
        eq_(camelcase2english('SeparatesMultipleWordsToo'),
            'Separates Multiple Words Too')

    def test_separates_two_words_lowercase_first(self):
        eq_(camelcase2english('twoWords'),
            'Two Words')

class Mock(object):
    def __init__(self, **kwargs):
        for attname, value in kwargs.iteritems():
            setattr(self, attname, value)

class TableMock(Mock):
    def __init__(self, name, columns=[], indexes=[], **kwargs):
        super(TableMock, self).__init__(
            name=name, columns=columns, indexes=indexes, **kwargs)

    def getColumns(self):
        return self.columns

class DatabaseMock(Mock):
    def table2model_name(self, t):
        return t

    def column2field_name(self, c, pk):
        return c

class ExampleDatabaseMock(DatabaseMock):
    reporter_id = Mock(name='id')
    reporter_table = TableMock('Reporter', [reporter_id])
    article_reporter = Mock(name='reporter_id')
    article_table = TableMock('Article', [article_reporter])
    newspaper_table = TableMock('Newspaper')
    publisher_table = TableMock('Publisher')

    _reporter_article_relationship = Mock(
        toTable=article_table,
        toColumns=[article_reporter],
        fromTable=reporter_table,
        fromColumns=[reporter_id],
        leftOuterJoin=False,
        rightOuterJoin=False,
        oneToOne=False)

    def getTableNames(self):
        return 'Reporter', 'Article', 'Newspaper', 'Publisher'

    def getTable(self, table_name):
        return getattr(self, '%s_table' % table_name.lower())

    def getRelationships(self, table1, table2):
        if table1.name == 'Reporter' and table2.name == 'Article':
            return [self._reporter_article_relationship]
        return []

def test_relationship():
    """Test the Relationship class

    The test uses strings to represent Jackcess objects
    """
    access_relationship = Mock(
        toTable='to_table', fromTable='from_table',
        toColumns=['to_column'], fromColumns=['from_column'])

    class ModelMock(Mock):
        def get_field_by_column(self, column):
            return '%s.%s' % (self.name, column.replace('column', 'field'))

    models = {'to_table': ModelMock(name='to_model'),
              'from_table': ModelMock(name='from_model')}

    dbwrapper = Mock(get_model_by_table=lambda t: models[t])
    r = Relationship(dbwrapper, access_relationship)
    eq_(r.to_field, 'to_model.to_field')
    eq_(r.from_field, 'from_model.from_field')

class ForeignKey_Tests:
    def setUp(self):
        db = DatabaseMock(relationships={},
                          reverse_relationships={})
        self.f = Field(
            model=Mock(database=db,
                       table=Mock(name='Article'),
                       single_column_indexes={}),
            column=Mock(name='reporter_code'))
        relationship = Mock(
            from_field=Mock(name='code', model=Mock(name='Reporter')))
        db.relationships[self.f] = relationship
        db.reverse_relationships[self.f] = ['dummy']

    def test_foreign_key(self):
        assert_true(self.f.foreign_key)

    def test_field_class(self):
        eq_(self.f.field_class, 'ForeignKey')

    def test_to_model(self):
        eq_(list(self.f.attrs)[0], 'Reporter')

    def test_to_field(self):
        eq_(list(self.f.attrs)[1], "to_field='code'")

    def test_primary_key(self):
        eq_(self.f.primary_key, False)

    def test_reverse_foreign_keys(self):
        eq_(list(self.f.reverse_foreign_keys), ['dummy'])

class CharField_Tests:
    def setUp(self):
        database = ExampleDatabaseMock()
        db = DatabaseWrapper(database)
        self.f = Field(
            model=Model(
                database=db,
                access_table=TableMock(
                    'Reporter', database=database, indexes={})),
            column=Mock(name='id',
                        type=Mock(name=lambda: u'TEXT'),
                        length=100))

    def test_field_class(self):
        eq_(self.f.field_class, 'CharField')

    def test_max_length(self):
        eq_(list(self.f.attrs)[1], 'max_length=100')

class Model_Tests:
    def test_single_column_indexes(self):
        index = Mock(columns=[Mock(name='reporter_id')])
        m = Model(
            database=DatabaseWrapper(ExampleDatabaseMock()),
            access_table=TableMock('sometable', indexes=[index]))
        eq_(m.single_column_indexes, {'reporter_id': index})

    def test_fields(self):
        table = TableMock('Article', [Mock(name='title'),
                                      Mock(name='reporter')])
        article_model = Model(DatabaseMock(), table)
        eq_([(f.__class__.__name__, f.name) for f in article_model.fields],
            [('PrimaryKeyField', 'id'),
             ('Field', 'title'),
             ('Field', 'reporter')])

    def test_foreign_keys(self):
        db = DatabaseMock(relationships={})
        table = TableMock('Article', [Mock(name='title'),
                                      Mock(name='reporter')])
        # an AutoField primary key will be automatically created as
        # field #0
        article_model = Model(db, table)
        db.relationships[article_model.fields[2]] = 'the only foreign key'
        assert_true(article_model.fields[2] in db.relationships)
        assert_true(article_model.fields[2].foreign_key)
        eq_(list(article_model.foreign_keys), ['the only foreign key'])

    def test_inlines_as_python(self):
        table = TableMock('Article', [Mock(name='code')], indexes=[])
        database = DatabaseMock(relationships={})
        article_model = Model(database, table)
        database.relationships[article_model.fields[1]] = (
            Mock(to_field=Mock(name='article',
                               inline_class_name='ArticleInline')))
        eq_(list(article_model.inlines_as_python()), [
                '',
                'class ArticleInline(admin.TabularInline):',
                '    model = Article'])

    def test_inline_class_names_without_field_names(self):
        db = DatabaseMock(reverse_relationships={})
        reporter_model = Model(
            db, TableMock('Reporter', [Mock(name='code')]))
        db.reverse_relationships[reporter_model.fields[1]] = [
            Mock(to_field=Mock(inline_class_name='ArticleInline')),
            Mock(to_field=Mock(inline_class_name='OtherInline'))]
        eq_(list(reporter_model.inline_class_names),
            ['ArticleInline', 'OtherInline'])

class DatabaseWrapper_Tests:
    def test_add_relationships(self):
        d = DatabaseWrapper(ExampleDatabaseMock())
        relationships = {'all': {}}
        d._add_relationships(relationships, ['Reporter', 'Article'])
        key = 'Article', 'reporter_id'
        r = relationships['all']
        eq_([[f.name for f in key] for key in r.keys()],
            [['reporter_id', 'id']])
        eq_([(rs.to_field.name, rs.from_field.name) for rs in r.values()],
            [('reporter_id', 'id')])

    def test_relationships(self):
        rs = DatabaseWrapper(ExampleDatabaseMock()).relationships
        eq_([field.name for field in rs.keys()],
            ['reporter_id'])
        eq_([(r.to_field.name, r.from_field.name) for r in rs.values()],
            [('reporter_id', 'id')])
        eq_([(r.to_field.model.name, r.from_field.model.name)
             for r in rs.values()],
            [('Article', 'Reporter')])

    def test_reverse_relationships(self):
        rs = DatabaseWrapper(ExampleDatabaseMock()).reverse_relationships
        eq_([field.name for field in rs.keys()],
            ['id'])
        eq_([[(r.to_field.name, r.from_field.name) for r in x]
             for x in rs.values()],
            [[('reporter_id', 'id')]])
        eq_([[(r.to_field.model.name, r.from_field.model.name)
              for r in x] for x in rs.values()],
            [[('Article', 'Reporter')]])

    def test_models(self):
        d = ExampleDatabaseMock()
        ms = list(DatabaseWrapper(d).models)
        eq_([m.__class__ for m in ms], [Model, Model, Model, Model])
        eq_([m.access_table for m in ms],
            [d.reporter_table, d.article_table,
             d.newspaper_table, d.publisher_table])

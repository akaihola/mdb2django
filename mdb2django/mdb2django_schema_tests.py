from nose.tools import eq_, assert_true, assert_false, assert_raises

from mdb2django_schema import (
    forloop,
    camelcase2english,
    Relationship,
    DatabaseWrapper,
    Model,
    Field)

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
    reporter_table = TableMock('Reporter', [Mock(name='id')])
    article_table = TableMock('Article', [Mock(name='reporter_id')])
    newspaper_table = TableMock('Newspaper')
    publisher_table = TableMock('Publisher')

    _reporter_article_relationship = Mock(
        toTable=article_table,
        toColumns=[Mock(name='reporter_id')],
        fromTable=reporter_table,
        fromColumns=[Mock(name='id')],
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
        def get_field_for_column(self, column):
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
        table = TableMock('Article', [Mock(name='id'),
                                      Mock(name='reporter')])
        article_model = Model(DatabaseMock(), table)
        eq_([repr(f) for f in article_model.fields],
            ['<Field Article.id>', '<Field Article.reporter>'])

    def test_foreign_keys(self):
        db = Mock(relationships={})
        table = TableMock('Article', [Mock(name='id'),
                                      Mock(name='reporter')])
        article_model = Model(db, table)
        db.relationships[article_model.fields[1]] = 'the only foreign key'
        eq_(list(article_model.foreign_keys), ['the only foreign key'])

    def test_inlines_as_python(self):
        table = TableMock('Article', [Mock(name='id')], indexes=[])
        database = DatabaseMock(relationships={})
        article_model = Model(database, table)
        database.relationships[article_model.fields[0]] = (
            Mock(to_field=Mock(name='article',
                               inline_class_name='ArticleInline')))
        eq_(list(article_model.inlines_as_python()), [
                '',
                'class ArticleInline(admin.TabularInline):',
                '    model = Article'])

    def test_inline_class_names_without_field_names(self):
        db = Mock(reverse_relationships={})
        reporter_model = Model(
            db, TableMock('Reporter', [Mock(name='id')]))
        db.reverse_relationships[reporter_model.fields[0]] = [
            Mock(to_field=Mock(inline_class_name='ArticleInline')),
            Mock(to_field=Mock(inline_class_name='OtherInline'))]
        eq_(list(reporter_model.inline_class_names),
            ['ArticleInline', 'OtherInline'])

class DatabaseWrapper_Tests:
    def test_add_relationships(self):
        d = DatabaseWrapper(ExampleDatabaseMock())
        d._relationships = {'all': {}}
        d._add_relationships(['Reporter', 'Article'])
        key = 'Article', 'reporter_id'
        r = d.get_relationships()['all']
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

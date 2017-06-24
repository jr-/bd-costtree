#include "OperatorNode.hpp"

#include <algorithm>
#include <cmath>

using std::string;
using std::deque;
using std::unordered_map;

/*class Expression {};
class AndExpression : public Expression {};
class OrExpression : public Expression {};
class EqualExpression: public Expression {};
class NotEqualExpression : public Expression {};
class GreaterExpression : public Expression {};
class LessExpression : public Expression {};*/

Table::Table(string name, unsigned int tuple_quantity) :
	_name(name),
	_tuple_quantity(tuple_quantity),
	_primary_index(std::pair<unsigned int, unsigned int>(0, 0))
	{}
Table::~Table(){}

/*Table::Table(const Table& to_copy) : _name(to_copy._name), _attributes(to_copy._attributes), _primary_key(to_copy._primary_key),  _tuple_quantity(to_copy._tuple_quantity), _foreign_keys(to_copy._foreign_keys), _primary_index(to_copy._primary_index), _secondary_indexes(to_copy._secondary_indexes), _ordered_by(to_copy._ordered_by)
{}*/

unsigned int Table::best_access_cost() const
{
	//does this make sense for Table? Maybe not
    return 0;
}

void Table::add_attribute(string attribute_name, type attribute_type, unsigned int size, unsigned int variability)
{
	//name must be unique
	auto got = _attributes.find(attribute_name);
	if(got != _attributes.end()) {
		//attribute already exists, cant be inserted
		//TODO should we raise an exception?
		return;
	}
	auto attribute_characteristics = std::tuple<type, unsigned int, unsigned int>(attribute_type, size, variability);
	auto to_add = std::pair<string, std::tuple<type, unsigned int, unsigned int>>(attribute_name, attribute_characteristics);
	_attributes.insert(to_add);
}

void Table::add_primary_key(deque<string> primary_key)
{
	//all the members of primary_key must be attributes
	for(string i : primary_key) {
		if(_attributes.find(i) == _attributes.end()) {
			//primary key must be an attribute!
			continue;
		}
		_primary_key.push_back(i);
	}
}

void Table::add_foreign_key(string attribute_name, string foreign_table_name)
{
	//attribute_name must be an attribute in this table
	if(_attributes.find(attribute_name) == _attributes.end()) {
		//attribute_name is not an attribute of this table
		return;
	}
	_foreign_keys.insert(std::pair<string, string>(attribute_name, foreign_table_name));
}

void Table::add_secondary_index(string attribute_name, unsigned int n, unsigned int fi)
{
	//attribute_name must be the name of an attribute of this table.
	if(_attributes.find(attribute_name) == _attributes.end()) {
		//attribute_name is not an attribute of this table
		return;
	}
	auto values = std::pair<unsigned int, unsigned int>(n, fi);
	_secondary_indexes.insert(std::pair<string, std::pair<unsigned int, unsigned int>>(attribute_name, values));
}

void Table::add_primary_index(unsigned int n, unsigned int fi)
{
	_primary_index = std::pair<unsigned int, unsigned int>(n, fi);
}

void Table::ordered_by(string attribute)
{
	//attribute must be an attribute of this table
	if(_attributes.find(attribute) == _attributes.end()) {
		//attribute_name is not an attribute of this table
		return;
	}
	_ordered_by = attribute;
}

Expression::Expression(){}
Expression::~Expression(){}

AndExpression::AndExpression(const Expression* left, const Expression* right) : _left(left), _right(right) {}

int AndExpression::tuple_quantity(const Table* table) const
{
	int nr = table->tuple_quantity();
	double cardinalitites = _left->cardinality(table) * _right->cardinality(table);
	return cardinalitites / nr;
}

double AndExpression::cardinality(const Table* table) const
{
	double cardinalitites = _left->cardinality(table) * _right->cardinality(table);
	return cardinalitites / table->tuple_quantity();
}

OrExpression::OrExpression(const Expression* left, const Expression* right) : _left(left), _right(right) {}

int OrExpression::tuple_quantity(const Table* table) const {}
double OrExpression::cardinality(const Table* table) const {}

EqualExpression::EqualExpression(const std::pair<string, string> left, const std::pair<string, string> right):  _left_attribute(left), _right_attribute(right) {}

int EqualExpression::tuple_quantity(const Table* table) const
{
	return this->cardinality(table);
}

double EqualExpression::cardinality(const Table* table) const
{
	double cardinality;
	if(_left_attribute.first == "") {
		cardinality = table->attribute_cardinality(_right_attribute.second);
	} else {
		cardinality = table->attribute_cardinality(_left_attribute.second);
	}
	return cardinality;
}

NotEqualExpression::NotEqualExpression(const std::pair<string, string> left, const std::pair<string, string> right) : _left_attribute(left), _right_attribute(right) {}

int NotEqualExpression::tuple_quantity(const Table* table) const
{
	return table->tuple_quantity() - this->cardinality(table) * table->tuple_quantity();
}

double NotEqualExpression::cardinality(const Table* table) const
{
	double cardinality;
	if(_left_attribute.first == "") {
		cardinality = table->attribute_cardinality(_right_attribute.second);
	} else {
		cardinality = table->attribute_cardinality(_left_attribute.second);
	}
	return cardinality;
}

GreaterExpression::GreaterExpression(const std::pair<string, string> left, const std::pair<string, string> right) : _left_attribute(left), _right_attribute(right) {}

int GreaterExpression::tuple_quantity(const Table* table) const
{
	return table->tuple_quantity() / 2;
}

double GreaterExpression::cardinality(const Table* table) const {}

LessExpression::LessExpression(const std::pair<string, string> left, const std::pair<string, string> right) : _left_attribute(left), _right_attribute(right) {}
int LessExpression::tuple_quantity(const Table* table) const {}
double LessExpression::cardinality(const Table* table) const {}

NaturalJoinNode::NaturalJoinNode(const Table* left, const Table* right) : Table("Join" + left->name() + right->name(), left->tuple_quantity() * right->tuple_quantity()), _left(left), _right(right)
{}

NaturalJoinNode::~NaturalJoinNode(){}

int NaturalJoinNode::best_access_cost()
{
	//we need to log all these results
	int res = A1();
	string best = "A1";
	int a2 = A2();
	if(a2 != 0 && a2 < res) {
		res = a2;
		best = "A2";
	}
	int a3 = A3();
	if(a3 != 0 && a3 < res) {
		res = a3;
		best = "A3";
	}

	int a4 = A4();
	if(a4 != 0 && a4 < res) {
		res = a4;
		best = "A4";
	}
	return res;
}

int NaturalJoinNode::A1()
{
	//is always possible
	int mult = _left->block_quantity() * _right->block_quantity();
	int res = std::min<int>(_left->block_quantity() + mult, _right->block_quantity() + mult);
	return res;
}

int NaturalJoinNode::A2()
{
	//when a2 cant be calculated, return 0
	if(!_left->has_primary_index() && !_right->has_primary_index()){
		return 0;
	}
	const Table* indexed, *no_index;
	if(_left->has_primary_index()){
		indexed = _left;
		no_index = _right;
	} else {
		indexed = _right;
		no_index = _left;
	}
	int res = no_index->block_quantity();
	res += no_index->tuple_quantity() * indexed->primary_index_access_cost();
	return res;
}

int NaturalJoinNode::A3()
{
    return 0;
}

int NaturalJoinNode::A4()
{
    return 0;
}

int Table::size() const
{
	int res = 0;
	for(std::pair<string, std::tuple<type, int, int>> i : _attributes) {
		//gets size
        res += std::get<1>(i.second);
	}
	return res;
}

unsigned int Table::primary_index_access_cost() const
{
	return 0; //TODO
}

SelectionNode::SelectionNode(const Table* child, const Expression* expr) : Table("Selection" + child->name(), 0/*Should be the estimation from the expression*/), _child(child), _expression(expr)
{}

SelectionNode::~SelectionNode(){}

int SelectionNode::best_access_cost()
{
	int bR;

	int res = A1();
	string best = "A1";

	bR = res;

	int a2 = A2(bR);
	if(a2 != 0 && a2 < res) {
		res = a2;
		best = "A2";
	}
	return 0;
}

/*
 * A1 = bR = Pesquisa linear
 * fR = piso[tbloco/tR] fator de bloco table
 * bR = teto[nR/fR] = Numero de blocos table
 * tbloco = tamanho do bloco BD
 * tR = tamanho da tupla table
 * nR = numero tuplas table
 *
 * @return bR
*/
int SelectionNode::A1()
{
	int fR = _block_size / _child->size();
	int bR = ceil(float(_child->tuple_quantity() / fR));

	return bR;
}

/*
 * A2 = teto[log2 bR] + teto[Cr(ai)/fR] -1 ou teto[log2 bR] se ai eh chave
 * Cr(ai) = nR/VR(ai)
 * fR = piso[tbloco/tR] fator de bloco table
 *
 *
 *
*/
int SelectionNode::A2(int bR)
{
	// qual a diferenca entre E e OU entre as expressoes, sempre considera a com menor custo pro resultado final?
	// data <= 'valor' ^ data >= 'valor'
	// (Expr ^ Expr) Expr
	//for(exp:Expression)
	//

    return 0;
}

ProductNode::ProductNode(const Table* left, const Table* right) : Table("Product" + left->name() + right->name(), left->tuple_quantity() * right->tuple_quantity()), _left(left), _right(right)
{}

ProductNode::~ProductNode(){}

int ProductNode::best_access_cost()
{
    int mult = _left->block_quantity() * _right->block_quantity();
	int res = std::min<int>(_left->block_quantity() + mult, _right->block_quantity() + mult);
	return res;
}

ProjectionNode::ProjectionNode(const Table* child, deque<std::pair<string, string>> attributes) : Table("Projection" + child->name(), 0), _child(child), _attribs(attributes)
{
    unordered_map<string, std::tuple<type, unsigned int, unsigned int>> at = child->get_attributes();
    unordered_map<string, std::tuple<type, unsigned int, unsigned int>>::const_iterator got;
    std::tuple<type, unsigned int, unsigned int> atf;
    for(auto &a : _attribs) {
        got = at.find(a.first+a.second);
        if ( got != at.end()) {
            atf = got->second;
            add_attribute(got->first, std::get<0>(atf), std::get<1>(atf), std::get<2>(atf));
        }
    }
}
ProjectionNode::~ProjectionNode(){}

int ProjectionNode::best_access_cost()
{
    return _child->block_quantity();
}

/*
class SelectionNode : public Table {

	public:

		int best_access_cost();

	private:
		//tree containing the expression
		Expression _expression;

};

class ProjectionNode : public Table {

	public:
		ProjectionNode(const Table* left, deque<string, string> attributes);
		virtual ~ProjectionNode();

		int best_access_cost();

	private:
		//attributes<attr name, table name> attributes to project
		deque<string, string> _attributes;

};

class ProductNode : public Table {

	public:
		ProductNode(const Table* left, const Table* right);
		virtual ~ProductNode();

		int best_access_cost();

};

class JoinNode : public Table {

	public:
		JoinNode(const Table* left, const Table* right, Expression expression);
		virtual ~JoinNode();

		int best_access_cost();

	private:
		Expression _expression;

};

class NaturalJoinNode : public Table {

	public:


};

class Database {

	public:
		Database(int nbuf, int block_size);
		void insert_table(Table table);

	private:
		//buffer size
		int _nBuf;
		//disk block size in bytes
		int _block_size;
		deque<Table> _tables;
};
*/

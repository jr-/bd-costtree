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

Table::Table(string name, int tuple_quantity) :
	_name(name),
	_tuple_quantity(tuple_quantity)
	{}
Table::~Table(){}

Table::Table(const Table& to_copy) : _name(to_copy._name), _attributes(to_copy._attributes), _primary_key(to_copy._primary_key),  _tuple_quantity(to_copy._tuple_quantity), _foreign_keys(to_copy._foreign_keys), _primary_index(to_copy._primary_index), _secondary_indexes(to_copy._secondary_indexes), _ordered_by(to_copy._ordered_by)
{}

int Table::best_access_cost() const
{
	//does this make sense for Table? Maybe not
    return 0;
}

void Table::add_attribute(string attribute_name, type attribute_type, int size, int variability)
{
	//name must be unique
}

void Table::add_primary_key(deque<string> primary_key)
{
	//all the members of primary_key must be attributes
}

void Table::add_foreign_key(string attribute_name, string foreign_table_name)
{
	//attribute_name must be an attribute in this table
}

void Table::add_secondary_index(string attribute_name, int n, int fi)
{
	//attribute_name must be the name of an attribute of this table. n and fi must be positive
}

void Table::add_primary_index(int n, int fi)
{
	//n and fi must be positive. unsigned int instead?
}

void Table::ordered_by(string attribute)
{
	//attribute must be an attribute of this table
}

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

int Table::primary_index_access_cost() const
{
	return 0; //TODO
}

SelectionNode::SelectionNode(const Table* child, const Expression* expr) : Table("Selection" + child->name(), expr->tuple_quantity()/*Should be the estimation from the expression*/), _child(child), _expression(expr)
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

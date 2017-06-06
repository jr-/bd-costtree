#include "OperatorNode.hpp"

#include <algorithm>

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

OperatorNode::OperatorNode() : Table("", 0){}
OperatorNode::~OperatorNode(){}

Table::Table(string name, int tuple_quantity) :
	_name(name),
	_tuple_quantity(tuple_quantity)
	{}
Table::~Table(){}

int Table::best_access_cost() const
{
	//does this make sense for Table? Maybe not
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

NaturalJoinNode::NaturalJoinNode(const OperatorNode* left, const OperatorNode* right)
{
	this->_left = left;
	this->_right = right;
}

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
	const OperatorNode* indexed, *no_index;
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
}

int NaturalJoinNode::A4()
{
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

/*
class SelectionNode : public OperatorNode {

	public:
		SelectionNode(const OperatorNode* left, Expression expression);
		virtual ~SelectionNode();

		int best_access_cost();

	private:
		//tree containing the expression
		Expression _expression;

};

class ProjectionNode : public OperatorNode {

	public:
		ProjectionNode(const OperatorNode* left, deque<string, string> attributes);
		virtual ~ProjectionNode();

		int best_access_cost();

	private:
		//attributes<attr name, table name> attributes to project
		deque<string, string> _attributes;

};

class ProductNode : public OperatorNode {

	public:
		ProductNode(const OperatorNode* left, const OperatorNode* right);
		virtual ~ProductNode();

		int best_access_cost();

};

class JoinNode : public OperatorNode {

	public:
		JoinNode(const OperatorNode* left, const OperatorNode* right, Expression expression);
		virtual ~JoinNode();

		int best_access_cost();

	private:
		Expression _expression;

};

class NaturalJoinNode : public OperatorNode {

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

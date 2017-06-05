#include "OperatorNode.hpp"

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

OperatorNode::OperatorNode(){}
OperatorNode::~OperatorNode(){}

Table::Table(string name, int tuple_quantity) :
	_name(name),
	_tuple_quantity(tuple_quantity)
	{}
Table::~Table(){}

Table Table::result() const
{
}
int Table::best_access_number() const
{
}

void Table::add_attribute(string attribute_name, type attribute_type, int size, int variability)
{
}
void Table::add_primary_key(deque<string> primary_key) {}
void Table::add_foreign_key(string attribute_name, string foreign_table_name) {}
void Table::add_secondary_index(string attribute_name, int n, int fi) {}
void Table::add_primary_index(int n, int fi) {}
void Table::ordered_by(string attribute) {}

/*
class SelectionNode : public OperatorNode {

	public:
		SelectionNode(const OperatorNode* left, Expression expression);
		virtual ~SelectionNode();

		int best_access_number();
		Table result();

	private:
		//tree containing the expression
		Expression _expression;

};

class ProjectionNode : public OperatorNode {

	public:
		ProjectionNode(const OperatorNode* left, deque<string, string> attributes);
		virtual ~ProjectionNode();

		int best_access_number();
		Table result();

	private:
		//attributes<attr name, table name> attributes to project
		deque<string, string> _attributes;

};

class ProductNode : public OperatorNode {

	public:
		ProductNode(const OperatorNode* left, const OperatorNode* right);
		virtual ~ProductNode();

		int best_access_number();
		Table result();

};

class JoinNode : public OperatorNode {

	public:
		JoinNode(const OperatorNode* left, const OperatorNode* right, Expression expression);
		virtual ~JoinNode();

		int best_access_number();
		Table result();

	private:
		Expression _expression;

};

class NaturalJoinNode : public OperatorNode {

	public:
		NaturalJoinNode(const OperatorNode* left, const OperatorNode* right);
		virtual ~NaturalJoinNode();

		int best_access_number();
		Table result();

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

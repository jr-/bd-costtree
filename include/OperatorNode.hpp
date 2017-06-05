#ifndef OPERATOR_NODE_HPP
#define OPERATOR_NODE_HPP

#include <string>
#include <deque>
#include <unordered_map>

using std::string;
using std::deque;
using std::unordered_map;

enum type {
	STRING, INT
};

class Expression {};
class AndExpression : public Expression {};
class OrExpression : public Expression {};
class EqualExpression: public Expression {};
class NotEqualExpression : public Expression {};
class GreaterExpression : public Expression {};
class LessExpression : public Expression {};

class Table;

class OperatorNode {

	public:
		OperatorNode();
		virtual ~OperatorNode();

		int best_access_number();
		Table result();

	protected:
		const OperatorNode* _left, *_right;
};

class Table : public OperatorNode {
	public:
		Table(string name, int tuple_quantity);
		virtual ~Table();

		Table result() const;
		int best_access_number() const;

		//must be unique
		void add_attribute(string attribute_name, type attribute_type, int size, int variability);
		//must be valid attributes already in the table
		void add_primary_key(deque<string> primary_key);
		void add_foreign_key(string attribute_name, string foreign_table_name);
		void add_secondary_index(string attribute_name, int n, int fi);
		void add_primary_index(int n, int fi);
		void ordered_by(string attribute);

	private:
		string _name;
		//attribute name indexes type, size in bytes and variability
		unordered_map<string, std::tuple<type, int, int>> _attributes;
		//must contain members of attributes
		deque<string> _primary_key;
		//tuple quantity
		int _tuple_quantity;
		//foreign keys. attribute name indexes table name
		unordered_map<string, string> _foreign_keys;
		//Does this table have a primary index? What is its N and Fi? If not, <0,0>
		std::pair<int, int> _primary_index;
		//secondary index<name, N, Fi>
		unordered_map<string, std::pair<int, int>> _secondary_indexes;
		//are the tuples ordered in disk by an attribute? If not, empty string
		string _ordered_by;

};

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

#endif  // OPERATOR_NODE_HPP

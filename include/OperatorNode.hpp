#ifndef OPERATOR_NODE_HPP
#define OPERATOR_NODE_HPP

#include <string>
#include <deque>
#include <unordered_map>
#include <tuple>
#include <math.h>

using std::string;
using std::deque;
using std::unordered_map;

enum type {
	STRING, INT, DATE
};

//global variable D:
extern int _block_size;
extern int _nBuf;

class Table {
	public:
		Table(string name, unsigned int tuple_quantity);
		//Table(const Table& to_copy);
		virtual ~Table();

		unsigned int best_access_cost() const;

		//must be unique
		void add_attribute(string attribute_name, type attribute_type, unsigned int size, unsigned int variability);
		//must be valid attributes already in the table
		void add_primary_key(deque<string> primary_key);
		void add_foreign_key(string attribute_name, string foreign_table_name);
		void add_secondary_index(string attribute_name, unsigned int n, unsigned int fi);
		void add_secondary_hash_index(string attribute_name)
			{_secondary_indexes. insert(std::pair<string, std::pair<unsigned int, unsigned int>>(attribute_name, std::pair<unsigned int, unsigned int>(0 ,1)));}
		void add_primary_index(unsigned int n, unsigned int fi);
		void ordered_by(string attribute);

		string name() const {return _name;};
		int tuple_quantity() const {return _tuple_quantity;};
		int block_quantity() const {return ceil(_tuple_quantity / block_factor());};
		int block_factor() const {return floor(_block_size / size());};
		int size() const;
		bool has_primary_index() const {return _primary_index != std::pair<unsigned int, unsigned int>(0,0);};
		unsigned int primary_index_access_cost() const;
		double attribute_cardinality(string attribute_name) const {return _tuple_quantity/std::get<2>(_attributes.at(attribute_name));};
        unordered_map<string, std::tuple<type, unsigned int, unsigned int>> get_attributes() const {return _attributes;};

	private:
		string _name;
		//attribute name indexes type, size in bytes and variability
		unordered_map<string, std::tuple<type, unsigned int, unsigned int>> _attributes;
		//must contain members of attributes
		deque<string> _primary_key;
		//tuple quantity
		unsigned int _tuple_quantity;
		//foreign keys. attribute name indexes table name
		unordered_map<string, string> _foreign_keys;
		//Does this table have a primary index? What is its N and Fi? If not, <0,0>. <0,1> indicate a hash index
		std::pair<unsigned int, unsigned int> _primary_index;
		//secondary index<name, N, Fi>
		unordered_map<string, std::pair<unsigned int, unsigned int>> _secondary_indexes;
		//are the tuples ordered in disk by an attribute? If not, empty string
		string _ordered_by;

};

class Expression {
	public:
		Expression();
		virtual ~Expression();
		virtual int tuple_quantity(const Table* table) const = 0;
		virtual double cardinality(const Table* table) const = 0;
};

class AndExpression : public Expression {
	public:
		AndExpression(const Expression* left, const Expression* right);

		int tuple_quantity(const Table * table) const;
		double cardinality(const Table* table) const;
	private:
		const Expression *_left, *_right;
};

class OrExpression : public Expression {
	public:
		OrExpression(const Expression* left, const Expression* right);

		int tuple_quantity(const Table * table) const;
		double cardinality(const Table* table) const;
	private:
		const Expression *_left, *_right;
};

class EqualExpression: public Expression {
	public:
		EqualExpression(const std::pair<string, string> left, const std::pair<string, string> right);

		int tuple_quantity(const Table * table) const;
		double cardinality(const Table* table) const;
	private:
		//contains <Table_name, attribute_name>
		//or just <"", value> for literals
		const std::pair<string, string> _left_attribute, _right_attribute;
};

class NotEqualExpression : public Expression {
	public:
		NotEqualExpression(const std::pair<string, string> left, const std::pair<string, string> right);

		int tuple_quantity(const Table * table) const;
		double cardinality(const Table* table) const;
	private:
		//contains <Table_name, attribute_name>
		//or just <"", value> for literals
		const std::pair<string, string> _left_attribute, _right_attribute;
};

class GreaterExpression : public Expression {
	public:
		GreaterExpression(const std::pair<string, string> left, const std::pair<string, string> right);

		int tuple_quantity(const Table * table) const;
		double cardinality(const Table* table) const;
	private:
		//contains <Table_name, attribute_name>
		//or just <"", value> for literals
		const std::pair<string, string> _left_attribute, _right_attribute;
};

class LessExpression : public Expression {
	public:
		LessExpression(const std::pair<string, string> left, const std::pair<string, string> right);

		int tuple_quantity(const Table * table) const;
		double cardinality(const Table* table) const;
	private:
		//contains <Table_name, attribute_name>
		//or just <"", value> for literals
		const std::pair<string, string> _left_attribute, _right_attribute;
};

class SelectionNode : public Table {

	public:
		SelectionNode(const Table* left, const Expression* expression);
		virtual ~SelectionNode();

		int best_access_cost();

	private:
		const Table *_child;
		//tree containing the expression
		const Expression* _expression;

		int A1();
		int A2(int bR);

};

class ProjectionNode : public Table {

	public:
		ProjectionNode(const Table* left, deque<std::pair<string, string>> attributes);
		virtual ~ProjectionNode();

		int best_access_cost();

	private:
		const Table *_child;
		//tablename e attribute name
		deque<std::pair<string, string>> _attribs;

};

class ProductNode : public Table {

	public:
		ProductNode(const Table* left, const Table* right);
		virtual ~ProductNode();

		int best_access_cost();

	private:
		const Table *_left, *_right; //children
};

class JoinNode : public Table {

	public:
		JoinNode(const Table* left, const Table* right, const Expression* expression);
		virtual ~JoinNode();

		int best_access_cost();

	private:
		const Table *_left, *_right; //children
		const Expression* _expression;

};

class NaturalJoinNode : public Table {

	public:
		NaturalJoinNode(const Table* left, const Table* right);
		virtual ~NaturalJoinNode();

		int best_access_cost();

	private:
		const Table *_left, *_right; //children
		int A1();
		int A2();
		int A3();
		int A4();
};

#endif  // OPERATOR_NODE_HPP

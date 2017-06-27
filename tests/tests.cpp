#include "gtest/gtest.h"

#include "OperatorNode.hpp"

using std::pair;

int _block_size = 1024;
int _nBuf = 5;

TEST(TableCreationTest, TableCreation) {
	Table tb("Table", 100);
	EXPECT_EQ("Table", tb.name());
	EXPECT_EQ(100, tb.tuple_quantity());
}

TEST(TrableTest, TableTest) {
	Table tb("Table", 100);
	tb.add_attribute("nome", STRING, 20, 50);
	EXPECT_EQ(100/50, tb.attribute_cardinality("nome"));
	tb.add_attribute("idade", INT, 2, 20);
	EXPECT_EQ(22, tb.size());
	auto empty = pair<unsigned int, unsigned int>(0,0);
	EXPECT_EQ(empty, tb.primary_index("nome"));
	tb.add_primary_index("nome", 5, 20);
	auto not_empty = pair<unsigned int, unsigned int>(5, 20);
	EXPECT_EQ(not_empty, tb.primary_index("nome"));
}

TEST(ExpressionTest, ExpressionTest) {
	Table tb("Table", 100);
	tb.add_attribute("name", STRING, 20, 50);
	EqualExpression eq = EqualExpression(std::pair<string, string>("Table", "name"), std::pair<string, string>("", "nicolas"));
	EXPECT_EQ(100/50, eq.tuple_quantity(&tb));
	tb.add_attribute("age", INT, 1, 20);
	GreaterExpression gt(std::pair<string, string>("Table", "age"), std::pair<string, string>("", "20"));
	EXPECT_EQ(100/2, gt.tuple_quantity(&tb));
}

TEST(ExpressionTest, IndexAccessCost) {
	int _old_block_size = _block_size;
	int _old_nbuf = _nBuf;
	_nBuf = 5;
	_block_size = 2048;
	Table tb("Teste", 1000);
	tb.add_attribute("codp", INT, 20, 1000);
	tb.add_primary_index("codp", 5, 10);
	EXPECT_EQ(3, tb.primary_index_access_cost("codp"));

	_nBuf = _old_nbuf;
	_block_size = _old_block_size;
}

TEST(ProjectionOperator, FullTest) {
    int _old_block_size = _block_size;
    _block_size = 1024;

    Table medicos("Medicos", 100);
    medicos.add_attribute("codm", INT, 5, 100);
    medicos.add_attribute("nome", STRING, 15, 100);
    medicos.add_attribute("cidade", STRING, 15, 50);
    medicos.add_attribute("idade", INT, 5, 40);
    medicos.add_attribute("especialidade", STRING, 10, 10);
    EXPECT_EQ(50, medicos.size());

    //ProjectionNode projection(deque<pair<string, string>>{pair<string, string>("Medicos", "codm"), pair<string, string>("Medicos", "nome")});
    ProjectionNode projection("Medicos.codm, Medicos.nome");
    projection.set_child(&medicos);

    EXPECT_EQ(5, projection.best_access_cost());
    EXPECT_EQ(20, projection.size());
    EXPECT_EQ(100, projection.tuple_quantity());
    EXPECT_EQ(2, projection.block_quantity());

	_block_size = _old_block_size;
}

TEST(ProductOperator, FullTest) {
    int _old_nbuf = _nBuf;
    int _old_block_size = _block_size;
    _nBuf = 5;
    _block_size = 1024;

    Table consultas("Consultas", 1000);
    consultas.add_attribute("codm", INT, 5, 80);
    consultas.add_attribute("codp", INT, 10, 450);
    consultas.add_attribute("data", DATE, 10, 300);
    consultas.add_attribute("hora", INT, 5, 15);

    EXPECT_EQ(30, consultas.size());

    consultas.add_secondary_index("codm", 5, 5);
    consultas.add_secondary_index("codp", 5, 5);
    consultas.add_secondary_index("data", 5, 5);
    consultas.add_secondary_hash_index("codm");
    consultas.add_secondary_hash_index("codp");

    consultas.ordered_by("data");
    consultas.add_foreign_key("codm", "Medicos");
    consultas.add_foreign_key("codp", "Pacientes");

    Table medicos("Medicos", 100);
    medicos.add_attribute("codm", INT, 5, 100);
    medicos.add_attribute("nome", STRING, 15, 100);
    medicos.add_attribute("cidade", STRING, 15, 50);
    medicos.add_attribute("idade", INT, 5, 40);
    medicos.add_attribute("especialidade", STRING, 10, 10);
    EXPECT_EQ(50, medicos.size());

    medicos.add_primary_key(deque<string>{"codm"});
    medicos.add_primary_index("codm", 10, 10);
    medicos.add_secondary_hash_index("especialidade");
    medicos.add_secondary_index("cidade", 5, 5);

    medicos.ordered_by("codm");

    ProductNode product;
    product.set_child_left(&medicos);
    product.set_child_right(&consultas);

    EXPECT_EQ(155, product.best_access_cost());
	EXPECT_EQ(100000, product.tuple_quantity());
	EXPECT_EQ(80, product.size());
    EXPECT_EQ(8334, product.block_quantity());

	_nBuf = _old_nbuf;
	_block_size = _old_block_size;
}

TEST(NaturalJoinNode, FullTest) {
    int _old_nbuf = _nBuf;
    int _old_block_size = _block_size;
    _nBuf = 5;
    _block_size = 1024;

    Table consultas("Consultas", 1000);
    consultas.add_attribute("codm", INT, 5, 80);
    consultas.add_attribute("codp", INT, 10, 450);
    consultas.add_attribute("data", DATE, 10, 300);
    consultas.add_attribute("hora", INT, 5, 15);

    EXPECT_EQ(30, consultas.size());

    consultas.add_secondary_index("codm", 5, 5);
    consultas.add_secondary_index("codp", 5, 5);
    consultas.add_secondary_index("data", 5, 5);
    consultas.add_secondary_hash_index("codm");
    consultas.add_secondary_hash_index("codp");

    consultas.ordered_by("data");
    consultas.add_foreign_key("codm", "Medicos");
    consultas.add_foreign_key("codp", "Pacientes");

    Table medicos("Medicos", 100);
    medicos.add_attribute("codm", INT, 5, 100);
    medicos.add_attribute("nome", STRING, 15, 100);
    medicos.add_attribute("cidade", STRING, 15, 50);
    medicos.add_attribute("idade", INT, 5, 40);
    medicos.add_attribute("especialidade", STRING, 10, 10);
    EXPECT_EQ(50, medicos.size());

    medicos.add_primary_key(deque<string>{"codm"});
	medicos.add_primary_index("codm", 10, 10);
    medicos.add_secondary_hash_index("especialidade");
    medicos.add_secondary_index("cidade", 5, 5);

    medicos.ordered_by("codm");

    NaturalJoinNode natural;
    natural.set_child_left(&medicos);
    natural.set_child_right(&consultas);

    EXPECT_EQ(155, natural.best_access_cost());//espera A1 best = 155 e A3 = 161
	EXPECT_EQ(1000, natural.tuple_quantity());
	EXPECT_EQ(75, natural.size());
    EXPECT_EQ(77, natural.block_quantity());

    _nBuf = _old_nbuf;
    _block_size = _old_block_size;
}

TEST(NaturalJoinNode, bestcostA3) {
    int _old_nbuf = _nBuf;
    int _old_block_size = _block_size;
    _nBuf = 5;
    _block_size = 1024;

    Table consultas("Consultas", 1000);
    consultas.add_attribute("codm", INT, 5, 80);
    consultas.add_attribute("codp", INT, 10, 450);
    consultas.add_attribute("data", DATE, 10, 300);
    consultas.add_attribute("hora", INT, 5, 15);

    EXPECT_EQ(30, consultas.size());

    consultas.add_secondary_index("codm", 5, 5);
    consultas.add_secondary_index("codp", 5, 5);
    consultas.add_secondary_index("data", 5, 5);
    consultas.add_secondary_hash_index("codm");
    consultas.add_secondary_hash_index("codp");

    consultas.ordered_by("codm");
    consultas.add_foreign_key("codm", "Medicos");
    consultas.add_foreign_key("codp", "Pacientes");

    Table medicos("Medicos", 100);
    medicos.add_attribute("codm", INT, 5, 100);
    medicos.add_attribute("nome", STRING, 15, 100);
    medicos.add_attribute("cidade", STRING, 15, 50);
    medicos.add_attribute("idade", INT, 5, 40);
    medicos.add_attribute("especialidade", STRING, 10, 10);
    EXPECT_EQ(50, medicos.size());

    medicos.add_primary_key(deque<string>{"codm"});
	medicos.add_primary_index("codm", 10, 10);
    medicos.add_secondary_hash_index("especialidade");
    medicos.add_secondary_index("cidade", 5, 5);

    medicos.ordered_by("codm");

    NaturalJoinNode natural;
    natural.set_child_left(&medicos);
    natural.set_child_right(&consultas);

    EXPECT_EQ(35, natural.best_access_cost());//espera A1 best = 155 e A3 = 35

    _nBuf = _old_nbuf;
    _block_size = _old_block_size;
}

TEST(NaturalJoinNode, tqnocommonat) {
    int _old_nbuf = _nBuf;
    int _old_block_size = _block_size;
    _nBuf = 5;
    _block_size = 1024;

    Table consultas("Consultas", 1000);
    consultas.add_attribute("codp", INT, 10, 450);
    consultas.add_attribute("data", DATE, 10, 300);
    consultas.add_attribute("hora", INT, 5, 15);

    Table medicos("Medicos", 100);
    medicos.add_attribute("codm", INT, 5, 100);

    NaturalJoinNode natural;
    natural.set_child_left(&medicos);
    natural.set_child_right(&consultas);

	EXPECT_EQ(100000, natural.tuple_quantity());

    _nBuf = _old_nbuf;
    _block_size = _old_block_size;
}

TEST(NaturalJoinNode, tqunique) {
    int _old_nbuf = _nBuf;
    int _old_block_size = _block_size;
    _nBuf = 5;
    _block_size = 1024;

    Table consultas("Consultas", 1000);
    consultas.add_attribute("codm", INT, 5, 1000);
    consultas.add_attribute("codp", INT, 10, 450);

    Table medicos("Medicos", 100);
    medicos.add_attribute("codm", INT, 5, 100);
    medicos.add_attribute("nome", STRING, 15, 100);

    NaturalJoinNode natural;
    natural.set_child_left(&medicos);
    natural.set_child_right(&consultas);

	EXPECT_EQ(100, natural.tuple_quantity());

    _nBuf = _old_nbuf;
    _block_size = _old_block_size;
}

TEST(NaturalJoinNode, tqatnokey) {
    int _old_nbuf = _nBuf;
    int _old_block_size = _block_size;
    _nBuf = 5;
    _block_size = 1024;

    Table consultas("Consultas", 1000);
    consultas.add_attribute("codm", INT, 5, 500);
    consultas.add_attribute("codp", INT, 10, 450);

    Table medicos("Medicos", 100);
    medicos.add_attribute("codm", INT, 5, 50);
    medicos.add_attribute("nome", STRING, 15, 100);

    NaturalJoinNode natural;
    natural.set_child_left(&medicos);
    natural.set_child_right(&consultas);

	EXPECT_EQ(200, natural.tuple_quantity());

    _nBuf = _old_nbuf;
    _block_size = _old_block_size;
}

TEST(JoinOperator, tqref) {
    int _old_nbuf = _nBuf;
    int _old_block_size = _block_size;
    _nBuf = 5;
    _block_size = 1024;

    Table consultas("Consultas", 1000);
    consultas.add_attribute("codm", INT, 5, 80);
    consultas.add_foreign_key("codm", "Medicos");

    Table medicos("Medicos", 100);
    medicos.add_attribute("codm", INT, 5, 100);

    //JoinExpression eq = JoinExpression(std::pair<string, string>("Medicos", "codm"), std::pair<string, string>("Consultas", "codm"), JoinExpression::JoinExpressionOperator::Equal);

    //JoinNode join(&eq);
    JoinNode join("Medicos.codm = Consultas.codm");
    // join.set_child_left(&medicos);
    // join.set_child_right(&consultas);
    //
	// EXPECT_EQ(1000, join.tuple_quantity()); //juncao por referencia = tuplas da tabela que contem a chave estrangeira
}

TEST(JoinOperator, tqunique) {
    int _old_nbuf = _nBuf;
    int _old_block_size = _block_size;
    _nBuf = 5;
    _block_size = 1024;

    Table consultas("Consultas", 1000);
    consultas.add_attribute("codm", INT, 5, 1000);

    Table medicos("Medicos", 100);
    medicos.add_attribute("codm", INT, 5, 100);

    JoinExpression eq = JoinExpression(std::pair<string, string>("Medicos", "codm"), std::pair<string, string>("Consultas", "codm"), JoinExpression::JoinExpressionOperator::Equal);

    JoinNode join(&eq);
    //JoinNode join("Medicos.codm = Consultas.codm");
    join.set_child_left(&medicos);
    join.set_child_right(&consultas);

	EXPECT_EQ(100, join.tuple_quantity()); //juncao por referencia = tuplas da tabela que contem a chave estrangeira
}

TEST(JoinOperator, tqeqnunique) {
    int _old_nbuf = _nBuf;
    int _old_block_size = _block_size;
    _nBuf = 5;
    _block_size = 1024;

    Table consultas("Consultas", 100);
    consultas.add_attribute("codm", INT, 5, 50);

    Table medicos("Medicos", 1000);
    medicos.add_attribute("codm", INT, 5, 1000);

    JoinExpression eq = JoinExpression(std::pair<string, string>("Medicos", "codm"), std::pair<string, string>("Consultas", "codm"), JoinExpression::JoinExpressionOperator::Equal);

    JoinNode join(&eq);
    //JoinNode join("Medicos.codm = Consultas.codm");
    join.set_child_left(&medicos);
    join.set_child_right(&consultas);

	EXPECT_EQ(200, join.tuple_quantity()); //juncao por referencia = tuplas da tabela que contem a chave estrangeira
}

TEST(JoinOperator, tqneqnunique) {
    int _old_nbuf = _nBuf;
    int _old_block_size = _block_size;
    _nBuf = 5;
    _block_size = 1024;

    Table consultas("Consultas", 200);
    consultas.add_attribute("codm", INT, 5, 50);

    Table medicos("Medicos", 100);
    medicos.add_attribute("codm", INT, 5, 100);

    JoinExpression eq = JoinExpression(std::pair<string, string>("Medicos", "codm"), std::pair<string, string>("Consultas", "codm"), JoinExpression::JoinExpressionOperator::NotEqual);

    JoinNode join(&eq);
    join.set_child_left(&medicos);
    join.set_child_right(&consultas);

	EXPECT_EQ(10000, join.tuple_quantity()); //juncao por referencia = tuplas da tabela que contem a chave estrangeira
}

TEST(JoinOperator, bacA1) {
    int _old_nbuf = _nBuf;
    int _old_block_size = _block_size;
    _nBuf = 5;
    _block_size = 1024;

    Table consultas("Consultas", 1000);
    consultas.add_attribute("codm", INT, 5, 80);
    consultas.add_attribute("codp", INT, 10, 450);
    consultas.add_attribute("data", DATE, 10, 300);
    consultas.add_attribute("hora", INT, 5, 15);

    EXPECT_EQ(30, consultas.size());

    consultas.add_secondary_index("codm", 5, 5);
    consultas.add_secondary_index("codp", 5, 5);
    consultas.add_secondary_index("data", 5, 5);
    consultas.add_secondary_hash_index("codm");
    consultas.add_secondary_hash_index("codp");

    consultas.ordered_by("data");
    consultas.add_foreign_key("codm", "Medicos");
    consultas.add_foreign_key("codp", "Pacientes");

    Table medicos("Medicos", 100);
    medicos.add_attribute("codm", INT, 5, 100);
    medicos.add_attribute("nome", STRING, 15, 100);
    medicos.add_attribute("cidade", STRING, 15, 50);
    medicos.add_attribute("idade", INT, 5, 40);
    medicos.add_attribute("especialidade", STRING, 10, 10);
    EXPECT_EQ(50, medicos.size());

    medicos.add_primary_key(deque<string>{"codm"});
	medicos.add_primary_index("codm", 10, 10);
    medicos.add_secondary_hash_index("especialidade");
    medicos.add_secondary_index("cidade", 5, 5);

    medicos.ordered_by("codm");

    JoinExpression eq = JoinExpression(std::pair<string, string>("Medicos", "codm"), std::pair<string, string>("Consultas", "codm"), JoinExpression::JoinExpressionOperator::NotEqual);

    JoinNode join(&eq);
    join.set_child_left(&medicos);
    join.set_child_right(&consultas);

    EXPECT_EQ(155, join.best_access_cost());//espera A1 best = 155 e A3 = 161

    _nBuf = _old_nbuf;
    _block_size = _old_block_size;
}

TEST(JoinOperator, bacA3) {
    int _old_nbuf = _nBuf;
    int _old_block_size = _block_size;
    _nBuf = 5;
    _block_size = 1024;

    Table consultas("Consultas", 1000);
    consultas.add_attribute("codm", INT, 5, 80);
    consultas.add_attribute("codp", INT, 10, 450);
    consultas.add_attribute("data", DATE, 10, 300);
    consultas.add_attribute("hora", INT, 5, 15);

    EXPECT_EQ(30, consultas.size());

    consultas.add_secondary_index("codm", 5, 5);
    consultas.add_secondary_index("codp", 5, 5);
    consultas.add_secondary_index("data", 5, 5);
    consultas.add_secondary_hash_index("codm");
    consultas.add_secondary_hash_index("codp");

    consultas.ordered_by("codm");
    consultas.add_foreign_key("codm", "Medicos");
    consultas.add_foreign_key("codp", "Pacientes");

    Table medicos("Medicos", 100);
    medicos.add_attribute("codm", INT, 5, 100);
    medicos.add_attribute("nome", STRING, 15, 100);
    medicos.add_attribute("cidade", STRING, 15, 50);
    medicos.add_attribute("idade", INT, 5, 40);
    medicos.add_attribute("especialidade", STRING, 10, 10);
    EXPECT_EQ(50, medicos.size());

    medicos.add_primary_key(deque<string>{"codm"});
	medicos.add_primary_index("codm", 10, 10);
    medicos.add_secondary_hash_index("especialidade");
    medicos.add_secondary_index("cidade", 5, 5);

    medicos.ordered_by("codm");

    JoinExpression eq = JoinExpression(std::pair<string, string>("Medicos", "codm"), std::pair<string, string>("Consultas", "codm"), JoinExpression::JoinExpressionOperator::NotEqual);

    JoinNode join(&eq);
    join.set_child_left(&medicos);
    join.set_child_right(&consultas);

    EXPECT_EQ(35, join.best_access_cost());//espera A1 best = 155 e A3 = 35

    _nBuf = _old_nbuf;
    _block_size = _old_block_size;
}

TEST(JoinOperator, sizeat) {
    int _old_nbuf = _nBuf;
    int _old_block_size = _block_size;
    _nBuf = 5;
    _block_size = 1024;

    Table consultas("Consultas", 1000);
    consultas.add_attribute("codm", INT, 5, 80);
    consultas.add_attribute("codp", INT, 10, 450);
    consultas.add_attribute("data", DATE, 10, 300);
    consultas.add_attribute("hora", INT, 5, 15);

    EXPECT_EQ(30, consultas.size());

    consultas.add_secondary_index("codm", 5, 5);
    consultas.add_secondary_index("codp", 5, 5);
    consultas.add_secondary_index("data", 5, 5);
    consultas.add_secondary_hash_index("codm");
    consultas.add_secondary_hash_index("codp");

    consultas.ordered_by("codm");
    consultas.add_foreign_key("codm", "Medicos");
    consultas.add_foreign_key("codp", "Pacientes");

    Table medicos("Medicos", 100);
    medicos.add_attribute("codm", INT, 5, 100);
    medicos.add_attribute("nome", STRING, 15, 100);
    medicos.add_attribute("cidade", STRING, 15, 50);
    medicos.add_attribute("idade", INT, 5, 40);
    medicos.add_attribute("especialidade", STRING, 10, 10);
    EXPECT_EQ(50, medicos.size());

    medicos.add_primary_key(deque<string>{"codm"});
	medicos.add_primary_index("codm", 10, 10);
    medicos.add_secondary_hash_index("especialidade");
    medicos.add_secondary_index("cidade", 5, 5);

    medicos.ordered_by("codm");

    JoinExpression eq = JoinExpression(std::pair<string, string>("Medicos", "codm"), std::pair<string, string>("Consultas", "codm"), JoinExpression::JoinExpressionOperator::NotEqual);

    JoinNode join(&eq);
    join.set_child_left(&medicos);
    join.set_child_right(&consultas);

    EXPECT_EQ(75, join.size());
    EXPECT_EQ(8, join.get_attributes().size());

    _nBuf = _old_nbuf;
    _block_size = _old_block_size;
}

TEST(SelectionTest, SecondaryIndexTest) {
	Table medicos("Medicos", 100);
	medicos.add_attribute("cidade", STRING, 50, 50);
	medicos.add_secondary_index("cidade", 5, 5);
	medicos.add_primary_key(deque<string>{"cidade"});
	NotEqualExpression not_equal(pair<string, string>("Medicos", "cidade"), pair<string, string>("", "Florianopolis"));
	SelectionNode sel(&not_equal);
	sel.set_child(&medicos);
	EXPECT_EQ(3, sel.best_access_cost()); //A6
	EXPECT_EQ(98, sel.tuple_quantity());
}

TEST(FullTest, FullTest) {
	int _old_nbuf = _nBuf;
	int _old_block_size = _block_size;
	_nBuf = 5;
	_block_size = 1024;

	Table consultas("Consultas", 1000);
	consultas.add_attribute("codm", INT, 5, 80);
	consultas.add_attribute("codp", INT, 10, 450);
	consultas.add_attribute("data", DATE, 10, 300);
	consultas.add_attribute("hora", INT, 5, 15);

	EXPECT_EQ(30, consultas.size());

	consultas.add_secondary_index("codm", 5, 5);
	consultas.add_secondary_index("codp", 5, 5);
	consultas.add_secondary_index("data", 5, 5);
	consultas.add_primary_index("data", 5, 5);
	consultas.add_secondary_hash_index("codm");
	consultas.add_secondary_hash_index("codp");

	consultas.ordered_by("data");
	consultas.add_foreign_key("codm", "Medicos");
	consultas.add_foreign_key("codp", "Pacientes");
	//Pacientes will not be used in this example, but will be put for completion sake
	Table medicos("Medicos", 100);
	medicos.add_attribute("codm", INT, 5, 100);
	medicos.add_attribute("nome", STRING, 15, 100);
	medicos.add_attribute("cidade", STRING, 15, 50);
	medicos.add_attribute("idade", INT, 5, 40);
	medicos.add_attribute("especialidade", STRING, 10, 10);
	EXPECT_EQ(50, medicos.size());

	medicos.add_primary_key(deque<string>{"codm"});
	medicos.add_primary_index("codm", 10, 10);
	medicos.add_secondary_hash_index("especialidade");
	medicos.add_secondary_index("cidade", 5, 5);

	medicos.ordered_by("codm");

	EqualExpression selectionLeftExpression(std::pair<string, string>("Consultas", "data"), std::pair<string, string>("", "15/10/2007"));
	SelectionNode selection_left(&selectionLeftExpression);
	selection_left.set_child(&consultas);
	ProjectionNode projection_left(deque<std::pair<string, string>>{std::pair<string, string>("Consultas", "codm")});
	projection_left.set_child(&selection_left);


	NotEqualExpression not_equal(pair<string, string>("Medicos", "cidade"), pair<string, string>("", "Florianopolis"));
	EqualExpression equal(pair<string, string>("Medicos", "especialidade"), pair<string, string>("", "ortopedista"));
	AndExpression selectionRightExpression(&equal, &not_equal);
	SelectionNode selection_right(&selectionRightExpression);
    selection_right.set_child(&medicos);
	ProjectionNode projection_right(deque<pair<string, string>>{pair<string, string>("Medicos", "codm"), pair<string, string>("Medicos", "nome")});
    projection_right.set_child(&selection_right);

	NaturalJoinNode natural_join;
    natural_join.set_child_left(&projection_left);
    natural_join.set_child_right(&projection_right);

	EXPECT_EQ(5, selection_right.best_access_cost()); //A1
	EXPECT_EQ(10, selection_right.tuple_quantity());
	EXPECT_EQ(500, selection_right.total_table_size());

	EXPECT_EQ(1, projection_right.best_access_cost());
	EXPECT_EQ(200, projection_right.total_table_size());

	EXPECT_EQ(4, selection_left.best_access_cost()); //A4
	EXPECT_EQ(4, selection_left.tuple_quantity());
	EXPECT_EQ(120, selection_left.total_table_size());

	EXPECT_EQ(1, projection_left.best_access_cost());
	EXPECT_EQ(20, projection_left.total_table_size());

	EXPECT_EQ(2, natural_join.best_access_cost());
	EXPECT_EQ(4, natural_join.tuple_quantity());
	EXPECT_EQ(80, natural_join.size());

	_nBuf = _old_nbuf;
	_block_size = _old_block_size;
}

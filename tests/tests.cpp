#include "gtest/gtest.h"

#include "OperatorNode.hpp"

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
	EXPECT_FALSE(tb.has_primary_index());
	tb.add_primary_index(5, 20);
	EXPECT_TRUE(tb.has_primary_index());
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

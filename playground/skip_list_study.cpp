//
// Created by yche on 10/24/18.
//

#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <algorithm>

// https://codereview.stackexchange.com/questions/116345/skip-list-implementation

class Skip_list {
public:
    Skip_list();

    ~Skip_list();

    // non-modifying member functions

    /*
    It prints the key, value, level
    of each node of the skip list.

    Prints two nodes per line.
    */
    void print() const;

    /*
    It searches the skip list and
    returns the element corresponding
    to the searchKey; otherwise it returns
    failure, in the form of null pointer.
    */
    std::string *find(int searchKey) const;

    // modifying member functions

    /*
    It searches the skip list for elements
    with seachKey, if there is an element
    with that key its value is reassigned to the
    newValue, otherwise it creates and splices
    a new node, of random level.
    */
    void insert(int searchKey, const std::string &newValue);

    /*
    It deletes the element containing
    searchKey, if it exists.
    */
    void erase(int searchKey);

private:
    struct Node {
        int key;
        std::string value;

        // pointers to successor nodes
        std::vector<Node *> forward;

        Node(int k, const std::string &v, int level) :
                key(k), value(v), forward(level, nullptr) {}
    };

    // Generates node levels in the range [1, maxLevel).
    int randomLevel() const;

    //Returns number of incoming and outgoing pointers
    static int nodeLevel(const Node *v);

    //creates a node on the heap and returns a pointer to it.
    static Node *makeNode(int key, std::string val, int level);

    // Returns the first node for which node->key < searchKey is false
    Node *lower_bound(int searchKey) const;

    /*
    * Returns a collection of Pointers to Nodes
    * result[i] hold the last node of level i+1 for which result[i]->key < searchKey is true
    */
    std::vector<Node *> predecessors(int searchKey) const;

    // data members
    const float probability;
    const int maxLevel;
    Node *head; // pointer to first node
    Node *NIL;  // last node
};


//==============================================================================
// Class Skip_list member implementations

Skip_list::Skip_list() :
        probability(0.5),
        maxLevel(16) {
    int headKey = std::numeric_limits<int>::min();
    head = new Node(headKey, "head", maxLevel);

    int nilKey = std::numeric_limits<int>::max();
    NIL = new Node(nilKey, "NIL", maxLevel);

    std::fill(head->forward.begin(), head->forward.end(), NIL);
}

Skip_list::~Skip_list() {
    auto node = head;
    while (node->forward[0]) {
        auto tmp = node;
        node = node->forward[0];
        delete tmp;
    }
    delete node;
}

std::string *Skip_list::find(int searchKey) const {
    std::string *res{};
    if (auto x = lower_bound(searchKey)) {
        if (x->key == searchKey && x != NIL) {
            res = &(x->value);
        }
    }
    return res;
}

void Skip_list::print() const {
    Node *list = head->forward[0];
    int lineLenght = 0;

    std::cout << "{";

    while (list != NIL) {
        std::cout << "value: " << list->value
                  << ", key: " << list->key
                  << ", level: " << nodeLevel(list);

        list = list->forward[0];

        if (list != NIL) std::cout << " : ";

        if (++lineLenght % 2 == 0) std::cout << "\n";
    }
    std::cout << "}\n";
}

void Skip_list::insert(int searchKey, const std::string &newValue) {
    auto preds = predecessors(searchKey);

    {//reassign value if node exists and return
        auto next = preds[0]->forward[0];
        if (next->key == searchKey && next != NIL) {
            next->value = newValue;
            return;
        }
    }

    // create new node
    const int newNodeLevel = randomLevel();
    auto newNodePtr = makeNode(searchKey, newValue, newNodeLevel);

    // connect pointers of predecessors and new node to respective successors
    for (int i = 0; i < newNodeLevel; ++i) {
        newNodePtr->forward[i] = preds[i]->forward[i];
        preds[i]->forward[i] = newNodePtr;
    }
}


void Skip_list::erase(int searchKey) {
    auto preds = predecessors(searchKey);

    //check if the node exists
    auto node = preds[0]->forward[0];
    if (node->key != searchKey || node == NIL) {
        return;
    }

    // update pointers and delete node
    for (size_t i = 0; i < nodeLevel(node); ++i) {
        preds[i]->forward[i] = node->forward[i];
    }
    delete node;
}

//###### private member functions ######
int Skip_list::nodeLevel(const Node *v) {
    return v->forward.size();
}

Skip_list::Node *Skip_list::makeNode(int key, std::string val, int level) {
    return new Node(key, val, level);
}

int Skip_list::randomLevel() const {
    int v = 1;
    while (((double) std::rand() / RAND_MAX) < probability &&
           v < maxLevel) {
        v++;
    }
    return v;
}

Skip_list::Node *Skip_list::lower_bound(int searchKey) const {
    Node *x = head;

    for (unsigned int i = nodeLevel(head); i-- > 0;) {
        while (x->forward[i]->key < searchKey) {
            x = x->forward[i];
        }
    }
    return x->forward[0];
}

std::vector<Skip_list::Node *> Skip_list::predecessors(int searchKey) const {
    std::vector<Node *> result(nodeLevel(head), nullptr);
    Node *x = head;

    for (unsigned int i = nodeLevel(head); i-- > 0;) {
        while (x->forward[i]->key < searchKey) {
            x = x->forward[i];
        }
        result[i] = x;
    }
    return result;
}

//==================================================
int main() {

    // 1.Initialize an empty Skip_list object
    Skip_list s;

    // 2. insert()
    for (int i = 0; i < 50; ++i) {
        std::stringstream ss;
        ss << i;

        s.insert(i, ss.str());
    }

    // 2a. print()
    s.print();
    std::cout << std::endl;

    // 3. find()
    auto f = s.find(10);
    if (f) std::cout << "Node found!\nvalue: " << f << '\n';
    else std::cout << "Node NOT found!\n";

    // 4. insert() - reassign
    s.insert(40, "TEST");

    // 4a. print()
    s.print();
    std::cout << std::endl;

    // 5. erase()
    s.erase(40);

    // 5a. print();
    s.print();
    std::cout << std::endl;

    std::cout << "\nDone!\n";
//    getchar();
}
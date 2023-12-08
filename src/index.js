import { connect, close } from './connection.js';
import { promises as fs } from 'fs';

const db = await connect();
const usersCollection = db.collection('users');
const studentsCollection = db.collection('students');
const articlesCollection = db.collection('articles');

const run = async () => {
	try {
		// await insertDocuments();
		// await getUsersExample();
		// await task1();
		// await task2();
		// await task3();
		// await task4();
		// await task5();
		// await task6();
		// await task7();
		await task8();
		// await task9();
		// await task10();
		// await task11();
		// await task12();

		await close();
	} catch (err) {
		console.log('Error: ', err);
	}
};
run();

// #### Users
// - Get users example
async function getUsersExample() {
	try {
		const [allUsers, firstUser] = await Promise.all([
			usersCollection.find().toArray(),
			usersCollection.findOne(),
		]);

		console.log('allUsers', allUsers);
		console.log('firstUser', firstUser);
	} catch (err) {
		console.error('getUsersExample', err);
	}
}

// - Get all users, sort them by age (ascending), and return only 5 records with firstName, lastName, and age fields.
async function task1() {
	try {
		const result = await usersCollection
			.find({}, { projection: { firstName: 1, lastName: 1, age: 1 } })
			.sort({ age: 1 })
			.limit(5)
			.toArray();
		console.log('task1', result);
	} catch (err) {
		console.error('task1', err);
	}
}

// - Add new field 'skills: []" for all users where age >= 25 && age < 30 or tags includes 'Engineering'
async function task2() {
	try {
		const result = await usersCollection.updateMany(
			{
				$or: [
					{ age: { $gte: 25, $lt: 30 } },
					{ tags: { $in: ['Engineering'] } },
				],
			},
			{
				$set: { skills: [] },
			},
		);

		console.log('task2 (modifiedCount):', result.modifiedCount);
	} catch (err) {
		console.error('task2', err);
	}
}

// - Update the first document and return the updated document in one operation (add 'js' and 'git' to the 'skills' array)
//   Filter: the document should contain the 'skills' field
async function task3() {
	try {
		const result = await usersCollection.findOneAndUpdate(
			{
				skills: { $exists: true },
			},
			{
				$set: { skills: ['js', 'git'] },
			},
			{
				returnDocument: 'after',
			},
		);

		console.log('task3:', result);
	} catch (err) {
		console.error('task3', err);
	}
}

// - REPLACE the first document where the 'email' field starts with 'john' and the 'address state' is equal to 'CA'
//   Set firstName: "Jason", lastName: "Wood", tags: ['a', 'b', 'c'], department: 'Support'
async function task4() {
	try {
		const result = await usersCollection.findOneAndReplace(
			{
				email: { $regex: /^john/ },
				'address.state': 'CA',
			},
			{
				firstName: 'Jason',
				lastName: 'Wood',
				tags: ['a', 'b', 'c'],
				department: 'Support',
			},
			{
				returnDocument: 'after',
			},
		);

		console.log('task4:', result);
	} catch (err) {
		console.log('task4', err);
	}
}

// - Pull tag 'c' from the first document where firstName: "Jason", lastName: "Wood"
async function task5() {
	try {
		const result = await usersCollection.findOneAndUpdate(
			{
				firstName: 'Jason',
				lastName: 'Wood',
			},
			{
				$pull: { tags: 'c' },
			},
			{
				returnDocument: 'after',
			},
		);

		console.log('task5:', result);
	} catch (err) {
		console.log('task5', err);
	}
}

// - Push tag 'b' to the first document where firstName: "Jason", lastName: "Wood"
//   ONLY if the 'b' value does not exist in the 'tags'
async function task6() {
	try {
		const result = await usersCollection.findOneAndUpdate(
			{
				firstName: 'Jason',
				lastName: 'Wood',
				tags: { $nin: ['b'] },
			},
			{
				$push: { tags: 'b' },
			},
			{
				returnDocument: 'after',
			},
		);

		console.log('task6:', result);
	} catch (err) {
		console.log('task6', err);
	}
}

// - Delete all users by department (Support)
async function task7() {
	try {
		const result = await usersCollection.deleteMany({
			department: 'Support',
		});

		console.log('task7:', result);
	} catch (err) {
		console.log('task7', err);
	}
}

// #### Articles
// - Create new collection 'articles'. Using bulk write:
//   Create one article per each type (a, b, c)
//   Find articles with type a, and update tag list with next value ['tag1-a', 'tag2-a', 'tag3']
//   Add tags ['tag2', 'tag3', 'super'] to articles except articles with type 'a'
//   Pull ['tag2', 'tag1-a'] from all articles
async function task8() {
	try {
		await db.createCollection('articles');

		const operations = [
			{
				insertOne: {
					document: {
						name: 'Express - Introduction',
						description: 'An introduction to the Express framework',
						type: 'a',
						tags: ['Node.js', 'Web Development'],
					},
				},
			},
			{
				insertOne: {
					document: {
						name: 'Machine Learning in Finance',
						description:
							'Exploring the applications of machine learning in the finance industry',
						type: 'b',
						tags: ['Machine Learning', 'Finance'],
					},
				},
			},
			{
				insertOne: {
					document: {
						name: 'Healthy Eating Habits',
						description: 'Tips for maintaining a healthy diet and lifestyle',
						type: 'c',
						tags: ['Nutrition', 'Wellness'],
					},
				},
			},
			{
				updateMany: {
					filter: {
						type: 'a',
					},
					update: {
						$set: { tags: ['tag1-a', 'tag2-a', 'tag3'] },
					},
				},
			},
			{
				updateMany: {
					filter: {
						type: { $ne: 'a' },
					},
					update: {
						$push: { tags: { $each: ['tag2', 'tag3', 'super'] } },
					},
				},
			},
			{
				updateMany: {
					filter: {},
					update: {
						$pull: { tags: { $in: ['tag2', 'tag1-a'] } },
					},
				},
			},
		];

		const result = await articlesCollection.bulkWrite(operations);
		console.log('task8: ', result);
	} catch (err) {
		console.error('task8', err);
	}
}

// - Find all articles that contains tags 'super' or 'tag2-a'
async function task9() {
	try {
		const articlesCollection = db.collection('articles');

		const result = await articlesCollection
			.find({
				tags: { $in: ['super', 'tag2-a'] },
			})
			.toArray();
		console.log('task9: ', result);
	} catch (err) {
		console.log('task9', err);
	}
}

// #### Students Statistic (Aggregations)
// - Find the student who have the worst score for homework, the result should be [ { name: <name>, worst_homework_score: <score> } ]
async function task10() {
	try {
		const result = await studentsCollection
			.aggregate([
				{
					$unwind: '$scores',
				},
				{
					$match: { 'scores.type': 'homework' },
				},
				{
					$sort: { 'scores.score': 1 },
				},
				{
					$limit: 1,
				},
				{
					$project: {
						_id: 0,
						name: 1,
						worst_homework_score: '$scores.score',
					},
				},
			])
			.toArray();

		console.log('task10', result);
	} catch (err) {
		console.log('task10', err);
	}
}

// - Calculate the average score for homework for all students, the result should be [ { avg_score: <number> } ]
async function task11() {
	try {
		const result = await studentsCollection
			.aggregate([
				{
					$unwind: '$scores',
				},
				{
					$match: { 'scores.type': 'homework' },
				},
				{
					$group: {
						_id: null,
						avg_score: { $avg: '$scores.score' },
					},
				},
				{
					$project: {
						_id: 0,
					},
				},
			])
			.toArray();

		console.log('task11', result);
	} catch (err) {
		console.log('task11', err);
	}
}

// - Calculate the average score by all types (homework, exam, quiz) for each student, sort from the largest to the smallest value
async function task12() {
	try {
		const result = await studentsCollection
			.aggregate([
				{
					$unwind: '$scores',
				},
				{
					$group: {
						_id: {
							_id: '$_id',
							name: '$name',
						},
						avg_score: { $avg: '$scores.score' },
					},
				},
				{
					$sort: {
						avg_score: -1,
					},
				},
				{
					$project: {
						_id: 0,
						name: '$_id.name',
						avg_score: 1,
					},
				},
			])
			.toArray();

		console.log('task12', result);
	} catch (err) {
		console.log('task12', err);
	}
}

async function insertDocuments() {
	const users = await fs.readFile('./users.json', 'utf8');
	const students = await fs.readFile('./students.json', 'utf8');

	await usersCollection.insertMany(JSON.parse(users));
	await studentsCollection.insertMany(JSON.parse(students));
}

async function deleteDocuments() {
	await usersCollection.deleteMany({});
	await usersCollection.deleteMany({});
	await studentsCollection.deleteMany({});
}
